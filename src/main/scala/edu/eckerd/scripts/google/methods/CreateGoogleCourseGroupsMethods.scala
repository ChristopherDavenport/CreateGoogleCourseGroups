package edu.eckerd.scripts.google.methods

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.typesafe.scalalogging.LazyLogging
import edu.eckerd.google.api.services.directory.Directory
import edu.eckerd.google.api.services.directory.models.Group
import edu.eckerd.google.api.services.directory.models.Member
import slick.driver.JdbcProfile
import edu.eckerd.scripts.google.persistence.GoogleTables.googleGroups
import edu.eckerd.scripts.google.persistence.GoogleTables.googleGroupToUser
import edu.eckerd.scripts.google.persistence.GoogleTables.GoogleGroupsRow
import edu.eckerd.scripts.google.persistence.GoogleTables.GoogleGroupToUserRow
import slick.jdbc.GetResult
import language.implicitConversions
import concurrent.{ExecutionContext, Future}
import scala.util.Try
/**
  * Created by davenpcm on 6/30/16.
  */
trait CreateGoogleCourseGroupsMethods extends LazyLogging{
  val profile : slick.driver.JdbcProfile
  import profile.api._

  /**
    * The primary Method and only public method exposed by these sets of Methods.
    * It generates the correct set of google groups.
    *
    * Below is a chart of application flow
    *
    * CreateGoogleCourseGroup
    * |
    * |
    * V
    * getTermGroupInformationFromDatabase
    * |
    * |               /PartitionByGroup
    * V              / TransformToGroups
    * createGroupData
    * |
    * |
    * V
    * createFromGroupData
    * |
    * |                   <----- createGroupInDB ------
    * V                 /                              \
    * checkIfGroupExists                                createGoogleGroup
    * |                 \ No                           /
    * |                  ------->createNonExistentGroup
    * |Yes
    * V
    * createMembersFromGroupData
    * |                   \ transformProfessorToMember
    * |                    \ transformStudentsToMembers
    * V
    * checkIfMemberExists
    * |                  \ Yes
    * |No                 ------------------> Successful(Group, Member, 0)
    * V
    * createNonExistentMember
    * |
    * |
    * V
    * createGoogleMember
    * |
    * |
    * V
    * createMemberInDB
    * |
    * |
    * |
    * --------------------------------------> Successful(Group, Member, 1)
    *
    *
    * @param db The database to be used to update these groups - This needs to have a full install of Banner Student
    *           in order for all of the appropriate tables for the SQL query to be in place
    * @param directory The google Directory object
    * @param ec An execution context so that it can split futures as neccessary
    * @return A Future set of Group, Member, and Int representing the group the member is a part of and the number
    *         of rows affected by this run.
    */
  def CreateGoogleCourseGroups()(implicit db: JdbcProfile#Backend#Database,
                                 directory: Directory,
                                 ec: ExecutionContext): Future[Seq[(Group, Member, Int)]] = {
    for {
      classGroupMembers <- getTermGroupInformationFromDatabase
      groupData = createGroupData(classGroupMembers)
      result <- Future.traverse(groupData)(createFromGroupData)
    } yield for {
      seq <- result
      flat <- seq
    } yield flat
  }

  case class ClassGroupMember(
                             courseName: String,
                             courseEmail: String,
                             studentEmail: String,
                             professorEmail: String,
                             crn: String,
                             term: String
                             )

  case class GroupData(group: Group, classGroupMembers : Seq[ClassGroupMember])

  /**
    * This Class is Similar To a Group Except that rather than Options, it Contains All Required Values To Go
    * Into the Table. This is very useful because it allows us to make sure explicitly all the correct information is
    * present at Compile Time Rather than at Runtime.
    *
    * On further advancements to the Google API perhaps this will not be necessary.
    *
    */
  case class CompleteGroup(
                                    Name: String,
                                    Email: String,
                                    Id: String,
                                    Description: Option[String],
                                    MemberCount: Long,
                                    AdminCreated: Boolean
                                  )
  def fromCompleteGroupToGroup(completeGroup: CompleteGroup): Group = {
    Group(
      completeGroup.Name,
      completeGroup.Email,
      Some(completeGroup.Id),
      completeGroup.Description,
      Some(completeGroup.MemberCount),
      None,
      Some(completeGroup.AdminCreated)
    )
  }
  def fromGroupToCompleteGroup(group: Group): CompleteGroup = {
    CompleteGroup(
      group.name,
      group.email,
      group.id.get,
      group.description,
      group.directMemberCount.getOrElse(0),
      group.adminCreated.get
    )
  }

  private implicit val getClassGroupMemberResult = GetResult(r => ClassGroupMember(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  /**
    * The cornerstone of this operation hinges around an effective query to return the necessary rows.
    *
    * Important Information
    *
    * GTVSDAX -
    * The Internal Code 'ECTERM' and the
    * Internal Code Group 'ALIAS_UP', 'ALIAS_UP_XCRS', 'ALIAS_UR', 'ALIAS_UR_XCRS'
    * These values must be advanced in Banner for new Terms to be created.
    *
    * SSBSECT -
    * Students Must Be Enrolled In The Course
    *
    * SIRASGN -
    * The course must have a Primary Professor
    *
    * STVRSTS -
    * Student Must Be included in enrollment and not withdrawn in order to be built
    *
    * GOREMAL -
    * Student Email must be type 'CAS'
    * Professor Email must be of type 'CA'
    *
    * Takes the Max Effective Term And Generates
    *
    * COURSE_TITLE - SSBSECT_CRSE_TITLE or if not present SCBCRSE_TITLE
    *
    * ALIAS -
    * IMPORTANT -
    * A Lowercase Email Address - If this is not lowered when google returns the information it will be in
    * a different format than what was given to google. Emails entered to google should always be lowercase.
    * Example(
    *           es || 499 || - || 1 || - || fa || @eckerd.edu
    *         )
    *
    * STUDENT_EMAIL - address from GOREMAL
    *
    * PROFESSOR_EMAIL -  Address from GOREMAL
    *
    * GTVSDAX_EXTERNAL_CODE - This corresponds to the Term Code that the Group was built and will be used to automate
    * any other actions against groups of this term, such as deletion at a later date.
    *
    *
    * @param db The database needed to execute this query - Also important in the implicit val for getResult above that
    *           implicitly converts the tuples that are natively returned into the appropriate ClassGroupMember
    * @return A Sequence of ClassGroup Memebers To Be Used To Generate Groups
    */
  private def getTermGroupInformationFromDatabase(implicit db: Database): Future[Seq[ClassGroupMember]] = {
    val data = sql"""
  SELECT
  nvl(SSBSECT_CRSE_TITLE, x.SCBCRSE_TITLE) as COURSE_TITLE,
  lower(SSBSECT_SUBJ_CODE || SSBSECT_CRSE_NUMB || '-' || TO_CHAR(TO_NUMBER(SSBSECT_SEQ_NUMB)) ||
  '-' || decode(substr(SFRSTCR_TERM_CODE, -2, 1), 1, 'fa', 2, 'sp', 3, 'su') || '@eckerd.edu') as ALIAS,
  studentemail.GOREMAL_EMAIL_ADDRESS as STUDENT_EMAIL,
  professorEmail.GOREMAL_EMAIL_ADDRESS as PROFESSOR_EMAIL,
  SFRSTCR_CRN as CRN,
  GTVSDAX_EXTERNAL_CODE
FROM
  SFRSTCR
  INNER JOIN
  SSBSECT
  INNER JOIN SCBCRSE x
    ON SCBCRSE_CRSE_NUMB = SSBSECT_CRSE_NUMB
       AND SCBCRSE_SUBJ_CODE = SSBSECT_SUBJ_CODE
    ON SSBSECT_TERM_CODE = SFRSTCR_TERM_CODE
       AND SSBSECT_CRN = SFRSTCR_CRN
       AND SSBSECT_ENRL > 0
       AND REGEXP_LIKE(SSBSECT_SEQ_NUMB, '^-?\d+?$$')
  LEFT JOIN SIRASGN
    ON SIRASGN_TERM_CODE = SFRSTCR_TERM_CODE
       AND SIRASGN_CRN = SFRSTCR_CRN
       AND SIRASGN_PRIMARY_IND = 'Y'
       AND SIRASGN_PIDM is not NULL
  INNER JOIN STVRSTS
    ON STVRSTS_CODE = SFRSTCR_RSTS_CODE
       AND STVRSTS_INCL_SECT_ENRL = 'Y'
       AND STVRSTS_WITHDRAW_IND = 'N'
  INNER JOIN GOREMAL studentemail
    ON SFRSTCR_PIDM = GOREMAL_PIDM
    AND studentemail.GOREMAL_EMAL_CODE = 'CAS'
  INNER JOIN GOREMAL professorEmail
    ON SIRASGN_PIDM = professorEmail.GOREMAL_PIDM
    AND professorEmail.GOREMAL_EMAL_CODE = 'CA'
  INNER JOIN (
    SELECT DISTINCT GTVSDAX_EXTERNAL_CODE
    FROM GTVSDAX
    WHERE
      GTVSDAX_INTERNAL_CODE = 'ECTERM'
      AND GTVSDAX_INTERNAL_CODE_GROUP IN ('ALIAS_UP', 'ALIAS_UP_XCRS', 'ALIAS_UR', 'ALIAS_UR_XCRS')
    ) ON SFRSTCR_TERM_CODE = GTVSDAX_EXTERNAL_CODE
WHERE
  (SELECT MAX(SCBCRSE_EFF_TERM)
   FROM SCBCRSE y
   WHERE y.SCBCRSE_CRSE_NUMB = x.SCBCRSE_CRSE_NUMB
         AND y.SCBCRSE_SUBJ_CODE = x.SCBCRSE_SUBJ_CODE
  ) = x.SCBCRSE_EFF_TERM
ORDER BY alias asc
    """.as[ClassGroupMember]

    db.run(data)
  }

  /**
    * This function takes the information returned from the database and manipulates it into a GroupData type which
    * can be used to generate the groups and members as opposed to dealing with the information in mass
    *
    * After this transformation the original Data is irrelevant and is no longer used. GroupData is used from this
    * point forward.
    *
    * @param allMembersForCurrentTerms This is the set of information returned from the database
    * @return A group data object
    */
  def createGroupData(allMembersForCurrentTerms: Seq[ClassGroupMember]): Seq[GroupData] = {
    val mapOfMembersByGroup = partitionByGroup(allMembersForCurrentTerms)

    for {
      group <- transformToGroups(allMembersForCurrentTerms)
    } yield {
      GroupData(group, mapOfMembersByGroup(group.email))
    }
  }

  /**
    * This is a simple splitter function by creating groups and deleting any duplicates.
    * @param allMembersForCurrentTerms The set of all records returned from the database
    * @return  A Sequence of Unique Groups
    */
  def transformToGroups(allMembersForCurrentTerms: Seq[ClassGroupMember]):Seq[Group] = {
    val groups = for {
      member <- allMembersForCurrentTerms
    } yield Group(
      member.courseName,
      member.courseEmail
    )

    groups.distinct
  }

  /**
    * This partitions the groups into a Map so that the courseEmail can be used to extract all the Members
    * of a particular group in constant time. So it is in N time with regards to the amount of Unique Groups
    *
    * @param allMembersForCurrentTerms The set of all records returned from the database
    * @return A Map connecting courseEmail to all Members of that course.
    */
  def partitionByGroup(allMembersForCurrentTerms: Seq[ClassGroupMember]): Map[String, Seq[ClassGroupMember]]= {
    allMembersForCurrentTerms.groupBy(_.courseEmail)
  }


  /**
    * This is the core function where splitting down the two paths operates and allows us
    * to never communicate with google unless we need to create the group.
    *
    * First it checks if the group exists
    *
    * If if does not -
    * It creates the group in Google,
    * creates the group in the Database
    * and then returns to this function
    *
    * If it does -
    * We use the returned group to check if Members Exists
    *   If the Member Exists We Return Succesfully Completed
    *   If the Member Does Not Exist We Create It in Google and Then Create it in the Database
    *
    * @param groupData A groupData which contains group information and all members
    * @param db The database to perform this on
    * @param directory The directory to check against Google
    * @param ec The execution context to perform this on
    * @return A Future of Sequence of Group, Member, Int - Which is the return of the entire operation
    */
  private def createFromGroupData(groupData: GroupData)(implicit db: JdbcProfile#Backend#Database,
                                                directory: Directory,
                                                ec: ExecutionContext): Future[Seq[(Group, Member, Int)]] = {
    checkIfGroupExists(groupData).flatMap{

      case Some(groupOption) =>
        logger.debug(s"$groupOption exists - continuing to check members")
        Future.sequence {
          createMembersOfGroupData(groupData).map(member =>
            checkIfMemberExists(groupOption, member).flatMap {
                  case Some(memberRowGroupRowTuple) =>
                    logger.debug(s"$groupOption - $member exists - doing nothing")
                    Future.successful((fromCompleteGroupToGroup(groupOption), member, 0))
                  case None =>
                    logger.info(s"$groupOption - $member - does not exist - creating member")
                    createNonexistentMember(groupOption, member).map(tuple => (fromCompleteGroupToGroup(tuple._1), tuple._2, tuple._3))
            }
          )
        }

      case None =>
        logger.info(s"${groupData.group} does not exist - creating group")
        createNonExistentGroup(groupData).flatMap(createFromGroupData)

    }
  }

  def convertGroupRowToCompleteGroup(googleGroupsRow: GoogleGroupsRow): CompleteGroup = {
    val adminCreated = true
    CompleteGroup(
      googleGroupsRow.name,
      googleGroupsRow.email,
      googleGroupsRow.id,
      googleGroupsRow.desc,
      googleGroupsRow.count,
      adminCreated
    )
  }

  /**
    * This is the check function to make sure the group exists. It checks the email against the email in the
    * GOOGLE_GROUPS table and  if they match converts them ta a CompleteGroup which will be used For the Members
    * Joing
    *
    * @param groupData The group to check if exists
    * @param db The database to check against
    * @param ec The execution context to perform it in
    * @return An option of a Complete Group or None
    */
  private def checkIfGroupExists(groupData: GroupData)(implicit db: JdbcProfile#Backend#Database, ec: ExecutionContext): Future[Option[CompleteGroup]] = {
    val group = groupData.group

    val action = googleGroups.filter(g =>
      g.email === group.email
    ).result.headOption

    db.run(action).flatMap{
      case None => Future.successful(None)
      case Some(groupInTable) =>
        val complete = convertGroupRowToCompleteGroup(groupInTable)

        equalityCheck(groupData, groupInTable) match {
        case true => Future.successful( Option(complete))
        case false => createOrUpdateGroupInDB(
          complete,
          groupData.classGroupMembers.head.term,
          groupData.classGroupMembers.head.crn
        ).map(_ =>
          Some(complete)
        )
      }
    }
  }

  def equalityCheck(groupData: GroupData, googleGroupsRow: GoogleGroupsRow): Boolean = {
    val term = groupData.classGroupMembers.head.term
    val crn = groupData.classGroupMembers.head.crn
    val autoKeyForGroupData = term + "-" + crn

    val equalEmail = groupData.group.email == googleGroupsRow.email
    val equalAutoKey = Option(autoKeyForGroupData) == googleGroupsRow.autoKey
    val equalAutoTerm = Option(term) == googleGroupsRow.autoTermCode

    List(equalEmail, equalAutoKey, equalAutoTerm).forall(_ == true)
  }

  /**
    * This function first determines the term from the Group Members records,
    * then creates the group in Google,
    * then creates the Group in the database with the term listed by the Group Members
    *
    * This returns a GroupData only after having converted to a CompleteGroup so It can be guaranteed that
    * the groups in the creation Process will be complete
    *
    * @param groupData The GroupData
    * @param db The database to perform this one
    * @param directory The directory to manipulate Google
    * @param ec The execution context to perform this one
    * @return A GroupData Object with a group that has the requirements to be a CompleteGroup
    */
  private def createNonExistentGroup(groupData: GroupData)
                            (implicit db: JdbcProfile#Backend#Database,
                             directory: Directory,
                             ec: ExecutionContext): Future[GroupData] = {

    val term = groupData.classGroupMembers.head.term
    val crn = groupData.classGroupMembers.head.crn
    for {
      group <- createGoogleGroup(groupData.group)
      intCreated <- createOrUpdateGroupInDB( fromGroupToCompleteGroup(group) , term, crn)
    } yield groupData.copy(group = group)
  }

  /**
    * This creates the group in Google using the Directory and Execution Context Provided.
    * Error Handling is in place for common errors.
    * limitCap - There is not an amazing way to deal with the rateLimit on Google, so it is deal with explicitly. It
    * goes to sleep... I know it is horrifying, however I am open to suggestion.
    * alreadyExists - If we got here something went wrong. Primarily in the process of creating a group this program
    * must have been stopped before it updated the Database. No fear however - We can go and retrieve that information
    * from google to recover. If it doesnt exists that is very worrisome. And it is the appropriate time the throw an
    * error
    *
    * @param group The group to create
    * @param directory The directory to create it in
    * @param ec The execution context to branch executors from
    * @return The Group after it has been created
    */
  private def createGoogleGroup(group: Group)(implicit directory: Directory, ec: ExecutionContext): Future[Group] = Future {
    directory.groups.create(group)
  } recoverWith {
    case limitCap : GoogleJsonResponseException if limitCap.getLocalizedMessage.contains("exceeds") =>
      Thread.sleep(100)
      createGoogleGroup(group)
    case alreadyExists : GoogleJsonResponseException if alreadyExists.getLocalizedMessage.contains("already exists") =>
      val tryRecoverGroup = Try(directory.groups.get(group.email).get)
      if (tryRecoverGroup.isFailure) {
        logger.error(s"Group Already Exists For $group")
        Future.failed(alreadyExists)
      } else {
        Future.successful(tryRecoverGroup.get)
      }
  }

  def createGoogleGroupsRow(group:CompleteGroup, term: String, crn: String): GoogleGroupsRow = {
    GoogleGroupsRow(
      group.Id,
      "Y",
      group.Name,
      group.Email,
      group.MemberCount,
      group.Description,
      None,
      Some ("COURSE"),
      Some (term + "-" + crn),
      Some (term)
    )
  }

  /**
    * By using CompleteGroups we have type safety that the information is all present when it reaches this function.
    * This writes groups to the Database
    *
    *
    * Auto_Indicator = Y
    * Auto_Type = COURSE
    * Auto_Key = email without the address, kept for legace purposes
    * Auto_Term_Code = The Term That this Course is For
    *
    * @param group The Group To Create
    * @param term The term the group is for
    * @param db The Database to Write this to
    * @param ec The execution Context to for executors from
    * @return An integer representing the Number of Rows Affected.
    *         This result is ignored in the execution of this program
    */
  private def createOrUpdateGroupInDB(group: CompleteGroup, term: String, crn: String)
                             (implicit db: JdbcProfile#Backend#Database, ec: ExecutionContext) : Future[Int] = {

    val NewGroup = createGoogleGroupsRow(group, term, crn)

    logger.debug(s"Creating or Updating - $NewGroup")
    db.run(googleGroups.insertOrUpdate(NewGroup))
  }

  /**
    * Creates Members from both the Professor and the Students of the Group. The Two helper function explain why
    * each decision was made.
    * @param groupData The groupData to build the members from
    * @return A Sequence of Members to Iterate Over
    */
  def createMembersOfGroupData(groupData: GroupData): Seq[Member] = {
    transformStudentsToMembers(groupData) ++ Seq[Member](transformProfessorToMember(groupData))
  }

  /**
    * Each classGroupMembers has the professor record. So this method takes the first record and generates the
    * professor as the group owner of that group.
    * @param groupData A group and all its data
    * @return The Professor as a Member
    */
  def transformProfessorToMember(groupData: GroupData): Member = {
    val professorEmail = groupData.classGroupMembers.head.professorEmail
    Member(
      email = Some(professorEmail),
      role = "OWNER"
    )
  }

  /**
    * This generates a member by wrapping each group Member as a Member
    * @param groupData A group and all its data
    * @return A Sequence of all student members
    */
  def transformStudentsToMembers(groupData: GroupData): Seq[Member] = {
    for {
      member <- groupData.classGroupMembers
    } yield Member(Some(member.studentEmail), role = "MEMBER" )
  }

  /**
    * This is where we check if the member already exists in the database. It simply checks against both the googleGroup
    * and GoogleMembers Table for the student email and group email and then has a join on the groupID to the Id in the
    * groups Table
    *
    * Note: This is a good example of a Joined Query using Slick as you can see it returns natively a * unless we map
    * to specific rows. So In the case of a two table * we are given a tuple of the two types of Records
    * GoogleGroupToUserRow and GoogleGroupsRow
    *
    * @param group A Complete Group That Is required to have an email
    * @param member A Member which must have an email
    * @param db The database to check against
    * @return An Option of A Tuple of a GoogleMembersRow and a GoogleGroups Row. Will be none if nothing meets the
    *         criteria of the query.
    */
  private def checkIfMemberExists(group: CompleteGroup, member: Member)
                                 (implicit db: JdbcProfile#Backend#Database)
  : Future[Option[(GoogleGroupToUserRow, GoogleGroupsRow)]] = {

    val memberFilter = googleGroupToUser.filter(_.userEmail === member.email)
    val groupFilter = googleGroups.filter(_.email === group.Email)

    val tableJoin = memberFilter join groupFilter on (_.groupId === _.id)
    val action = tableJoin.result.headOption

    val f = db.run(action)
    f
  }

  /**
    * This creates a Member if they did not exist in the database already. This strings together creating
    * the group in Google and Creating the Group in the database and is the last function returned from the
    * "Money" - createFromGroupData
    * function that strings the whole program together
    * @param group The complete Group
    * @param member The Member To Create
    * @param db The database to create it in
    * @param directory The Google Directory
    * @param ec The execution context to fork futures from
    * @return The group, the createdMember Returned from Google, and the number of rows affected by the act of
    *         creating the member in the database - This should be 1.
    */
  private def createNonexistentMember(group: CompleteGroup, member: Member)
                                     (implicit db: JdbcProfile#Backend#Database,
                                      directory: Directory,
                                      ec: ExecutionContext): Future[(CompleteGroup, Member, Int)] = {
    for {
      googleMember <- createGoogleMember(group, member)
      created <- createMemberInDB(group, googleMember)
    } yield (group, googleMember, created)

  }

  /**
    * This creates the Member in Google. As this is near the end it is where the most things can go wrong. The complete
    * group data type has accounted for most of these problems however it is still possible that we are trying to
    * create a group that exists in Google but not in the database. We have accounted for that by
    * giving it a recovery option by finding the user account and constructing an artificial Member record from the
    * correct information. We Also have recovery for if we are hitting google too fast to artificially slow ourselves
    * down.
    * @param group The Group The Member is to be a part of
    * @param member The Member To create
    * @param directory The google directory to create these from
    * @param ec The execution context to fork futures from
    * @return A Member
    */
  private def createGoogleMember(group: CompleteGroup, member: Member)(implicit directory: Directory, ec: ExecutionContext)
  : Future[Member] = Future {
    directory.members.add(group.Email, member)
  } recoverWith {
    case limitCap : GoogleJsonResponseException if limitCap.getLocalizedMessage.contains("exceeds") =>
      Thread.sleep(100)
      createGoogleMember(group, member)
    case alreadyExists : GoogleJsonResponseException if alreadyExists.getLocalizedMessage.contains("already exists") =>
      logger.error(s"Member Already Exists For $group - $member")
      val extantMember = directory.members.list(group.Email).find(_.email.get == member.email.get)
      extantMember.map(Future.successful).getOrElse(Future.failed(alreadyExists))
  }

  /**
    * Creating the Member in the Database. The Try is a Relic of some Errors that were happening before I
    * created the CompleteGroup Data type. However it should still be useful in case errors pop up as its debug lets
    * us know explicitly where errors occur to investigate why they occur.
    * @param group A Complete Group
    * @param member A Member
    * @param db The Database to perform this on
    * @param ec The execution context to fork futures into
    * @return An Integer representing the number of rows affected. Since We only have created 1 record, this should
    *         only ever be 1.
    */
  private def createMemberInDB(group: CompleteGroup, member: Member)
                              (implicit db: JdbcProfile#Backend#Database, ec: ExecutionContext): Future[Int] = {

    val NewMember = GoogleGroupToUserRow(
      groupId = group.Id,
      member.id.get,
      member.email,
      "Y",
      member.role,
      member.memberType
    )
    val exists = db.run(
      googleGroupToUser.filter(row => row.groupId === group.Id && row.userID === NewMember.userID).exists.result
    )
    exists.flatMap {
      case true =>
        db.run(googleGroupToUser.filter(row => row.groupId === group.Id && row.userID === NewMember.userID).delete)
          .flatMap( _ =>
            db.run(googleGroupToUser += NewMember)
          )
      case false =>
        db.run(googleGroupToUser += NewMember)
    }
  }

}
