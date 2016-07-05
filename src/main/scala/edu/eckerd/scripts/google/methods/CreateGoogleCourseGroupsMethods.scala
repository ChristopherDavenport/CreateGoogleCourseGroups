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

  def CreateGoogleCourseGroups()(implicit db: JdbcProfile#Backend#Database,
                                 directory: Directory,
                                 ec: ExecutionContext): Future[Seq[Seq[(Group, Member, Int)]]] = {
    for {
      classGroupMembers <- getTermGroupInformationFromDatabase
      groupData = createGroupData(classGroupMembers)
      result <- Future.traverse(groupData)(createFromGroupData)
    } yield result
  }

  private case class ClassGroupMember(
                             courseName: String,
                             courseEmail: String,
                             studentEmail: String,
                             professorEmail: String,
                             term: String
                             )

  private case class GroupData(group: Group, classGroupMembers : Seq[ClassGroupMember])

  private implicit val getClassGroupMemberResult = GetResult(r => ClassGroupMember(r.<<, r.<<, r.<<, r.<<, r.<<))


  private def getTermGroupInformationFromDatabase(implicit db: Database): Future[Seq[ClassGroupMember]] = {


    val data = sql"""
  SELECT
  nvl(SSBSECT_CRSE_TITLE, x.SCBCRSE_TITLE) as COURSE_TITLE,
  lower(SSBSECT_SUBJ_CODE || SSBSECT_CRSE_NUMB || '-' || TO_CHAR(TO_NUMBER(SSBSECT_SEQ_NUMB)) ||
  '-' || decode(substr(SFRSTCR_TERM_CODE, -2, 1), 1, 'fa', 2, 'sp', 3, 'su') || '@eckerd.edu') as ALIAS,
  studentemail.GOREMAL_EMAIL_ADDRESS as STUDENT_EMAIL,
  professorEmail.GOREMAL_EMAIL_ADDRESS as PROFESSOR_EMAIL,
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


  private def transformToGroups(allMembersForCurrentTerms: Seq[ClassGroupMember]):Seq[Group] = {
    val groups = for {
      member <- allMembersForCurrentTerms
    } yield Group(
      member.courseName,
      member.courseEmail
    )

    groups.distinct
  }

  private def partitionByGroup(allMembersForCurrentTerms: Seq[ClassGroupMember]): Map[String, Seq[ClassGroupMember]]= {
    allMembersForCurrentTerms.groupBy(_.courseEmail)
  }

  private def createGroupData(allMembersForCurrentTerms: Seq[ClassGroupMember]): Seq[GroupData] = {
    val mapOfMembersByGroup = partitionByGroup(allMembersForCurrentTerms)

    for {
      group <- transformToGroups(allMembersForCurrentTerms)
    } yield {
      GroupData(group, mapOfMembersByGroup.get(group.email).get)
    }
  }


  private def createFromGroupData(groupData: GroupData)(implicit db: JdbcProfile#Backend#Database,
                                                directory: Directory,
                                                ec: ExecutionContext): Future[Seq[(Group, Member, Int)]] = {
    checkIfGroupExists(groupData.group).flatMap{

      case Some(groupOption) =>
        logger.debug(s"$groupOption exists - continuing to check members")
        Future.sequence {
          createMembersOfGroupData(groupData).map(member =>
            checkIfMemberExists(groupOption, member).flatMap {
              case Some(memberRowGroupRowTuple) =>
                logger.debug(s"$groupOption - $member - exist - doing nothing")
                Future.successful(( fromCompleteGroupToGroup(groupOption), member, 0))
              case None =>
                logger.debug(s"$groupOption - $member - does not exist - creating member")
                createNonexistentMember(groupOption, member).map(tuple => ( fromCompleteGroupToGroup(tuple._1), tuple._2, tuple._3))
            }
          )
        }

      case None =>
        logger.debug(s"${groupData.group} does not exist - creating group")
        createNonExistentGroup(groupData).flatMap(createFromGroupData)

    }
  }


  private def createNonExistentGroup(groupData: GroupData)
                            (implicit db: JdbcProfile#Backend#Database,
                             directory: Directory,
                             ec: ExecutionContext): Future[GroupData] = {
    val term = groupData.classGroupMembers.head.term
    for {
      group <- createGoogleGroup(groupData.group)
      intCreated <- createGroupInDB(group, term)
    } yield groupData.copy(group = group)
  }

  private def checkIfGroupExists(group: Group)(implicit db: JdbcProfile#Backend#Database, ec: ExecutionContext): Future[Option[CompleteGroup]] = {
    val action = googleGroups.filter(g =>
      g.email === group.email
    ).result.headOption

    db.run(action).map{
      case None => None
      case Some(groupInTable) => Some(convertGroupRowToCompleteGroup(groupInTable))
    }
  }

  private def convertGroupRowToCompleteGroup(googleGroupsRow: GoogleGroupsRow): CompleteGroup = {
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

  private def createGoogleGroup(group: Group)(implicit directory: Directory, ec: ExecutionContext): Future[Group] = Future {
    directory.groups.create(group)
  } recoverWith {
    case limitCap : GoogleJsonResponseException if limitCap.getLocalizedMessage.contains("exceeds") =>
      Thread.sleep(100)
      createGoogleGroup(group)
    case alreadyExists : GoogleJsonResponseException if alreadyExists.getLocalizedMessage.contains("already exists") =>
      logger.error(s"Group Already Exists For $group")
      Future.failed(alreadyExists)
  }

  private def createGroupInDB(group: CompleteGroup, term: String)(implicit db: JdbcProfile#Backend#Database, ec: ExecutionContext) : Future[Int] = {

    val NewGroup = Try{GoogleGroupsRow(
      group.Id,
      "Y",
      group.Name,
      group.Email,
      group.MemberCount,
      group.Description,
      None,
      Some ("COURSE"),
      Some (group.email.replace ("@eckerd.edu", "") ),
      Some (term)
    )}

    if (NewGroup.isFailure){
      logger.error(s"Failed to create $group in DB")
      Future.failed(NewGroup.failed.get)
    } else{
      logger.debug(s"Creating or Updating - $NewGroup")
      db.run(googleGroups.insertOrUpdate(NewGroup.get))
    }

  }



  private def createNonexistentMember(group: CompleteGroup, member: Member)(implicit db: JdbcProfile#Backend#Database,
                                                            directory: Directory,
                                                            ec: ExecutionContext): Future[(CompleteGroup, Member, Int)] = {
    for {
      googleMember <- createGoogleMember(group, member)
      created <- createMemberInDB(group, googleMember)
    } yield (group, googleMember, created)

  }

  private def createMemberInDB(group: CompleteGroup, member: Member)(implicit db: JdbcProfile#Backend#Database, ec: ExecutionContext): Future[Int] = {
    val NewMember = Try{GoogleGroupToUserRow(
      group.Id,
      member.id.get,
      member.email,
      "Y",
      member.role,
      member.memberType
    )
    }
    if (NewMember.isFailure){
      logger.error(s"Failed to create $group in DB")
      Future.failed(NewMember.failed.get)
    } else{
      logger.debug(s"Creating or Updating - $NewMember")
      db.run(googleGroupToUser += NewMember.get)
    }


  }

  private def createGoogleMember(group: CompleteGroup, member: Member)(implicit directory: Directory, ec: ExecutionContext)
  : Future[Member] = Future {
    directory.members.add(group.Email, member)
  } recoverWith {
    case limitCap : GoogleJsonResponseException if limitCap.getLocalizedMessage.contains("exceeds") =>
      Thread.sleep(100)
      createGoogleMember(group, member)
    case alreadyExists : GoogleJsonResponseException if alreadyExists.getLocalizedMessage.contains("already exists") =>
      logger.error(s"Member Already Exists For $group - $member")
      val user = directory.users.get(member.email.get)

      Future{ Member(member.email, user.right.get.id, member.memberType, member.role) }
  }

  private def checkIfMemberExists(group: CompleteGroup, member: Member)(implicit db: JdbcProfile#Backend#Database): Future[Option[(GoogleGroupToUserRow, GoogleGroupsRow)]] = {

    val memberFilter = googleGroupToUser.filter(_.userEmail === member.email)
    val groupFilter = googleGroups.filter(_.email === group.email)

    val tableJoin = memberFilter join groupFilter on (_.groupId === _.id)
    val action = tableJoin.result.headOption

    val f = db.run(action)
    f
  }

  private def createMembersOfGroupData(groupData: GroupData): Seq[Member] = {
    transformStudentsToMembers(groupData) ++ Seq[Member](transformProfessorToMember(groupData))
  }

  private def transformProfessorToMember(groupData: GroupData): Member = {
    val professorEmail = groupData.classGroupMembers.head.professorEmail
    Member(
      email = Some(professorEmail),
      role = "OWNER"
    )
  }

  private def transformStudentsToMembers(groupData: GroupData): Seq[Member]
  = {
    for {
      member <- groupData.classGroupMembers
    } yield Member(Some(member.studentEmail))
  }

  private case class CompleteGroup(
                                  Name: String,
                                  Email: String,
                                  Id: String,
                                  Description: Option[String],
                                  MemberCount: Long,
                                  AdminCreated: Boolean
                                  )

  private implicit def fromCompleteGroupToGroup(completeGroup: CompleteGroup): Group = {
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

  private implicit def fromGroupToCompleteGroup(group: Group): CompleteGroup = {
    CompleteGroup(
      group.name,
      group.email,
      group.id.get,
      group.description,
      group.directMemberCount.get,
      group.adminCreated.get
    )
  }




}
