package edu.eckerd.scripts.google.methods

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import edu.eckerd.google.api.services.directory.Directory
import edu.eckerd.google.api.services.directory.models.Group
import edu.eckerd.google.api.services.directory.models.Member
import slick.driver.JdbcProfile
import edu.eckerd.scripts.google.persistence.GoogleTables.googleGroups
import edu.eckerd.scripts.google.persistence.GoogleTables.googleGroupToUser
import edu.eckerd.scripts.google.persistence.GoogleTables.GoogleGroupsRow
import edu.eckerd.scripts.google.persistence.GoogleTables.GoogleGroupToUserRow
import slick.jdbc.GetResult

import concurrent.{ExecutionContext, Future}
/**
  * Created by davenpcm on 6/30/16.
  */
trait CreateGoogleCourseGroupsMethods {
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
  lower(SSBSECT_SUBJ_CODE) || SSBSECT_CRSE_NUMB || '-' || TO_CHAR(TO_NUMBER(SSBSECT_SEQ_NUMB)) ||
  '-' || decode(substr(SFRSTCR_TERM_CODE, -2, 1), 1, 'fa', 2, 'sp', 3, 'su') || '@eckerd.edu' as ALIAS,
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
      case true => Future.sequence {
        createMembersOfGroupData(groupData).map(member =>
          checkIfMemberExists(groupData.group, member).flatMap {
            case true =>
              Future.successful((groupData.group, member, 0))
            case false =>
              createNonexistentMember(groupData.group, member)
          }
        )
      }
      case false =>
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

  private def checkIfGroupExists(group: Group)(implicit db: JdbcProfile#Backend#Database, ec: ExecutionContext): Future[Boolean] = {
    val action = googleGroups.withFilter(_.email === group.email).exists.result
    db.run(action)
  }

  private def createGoogleGroup(group: Group)(implicit directory: Directory, ec: ExecutionContext): Future[Group] = Future {
    directory.groups.create(group)
  } recoverWith {
    case limitCap : GoogleJsonResponseException if limitCap.getLocalizedMessage.contains("exceeds") =>
      Thread.sleep(1000)
      createGoogleGroup(group)
  }

  private def createGroupInDB(group: Group, term: String)(implicit db: JdbcProfile#Backend#Database, ec: ExecutionContext) : Future[Int] = {
    val NewGroup = GoogleGroupsRow(
      group.id.get,
      "Y",
      group.name,
      group.email,
      group.directMemberCount.getOrElse(0),
      group.description,
      None,
      Some ("COURSE"),
      Some (group.email.replace ("@eckerd.edu", "") ),
      Some (term)
    )

    db.run(googleGroups += NewGroup)
  }



  private def createNonexistentMember(group: Group, member: Member)(implicit db: JdbcProfile#Backend#Database,
                                                            directory: Directory,
                                                            ec: ExecutionContext): Future[(Group, Member, Int)] = {
    for {
      googleMember <- createGoogleMember(group, member)
      created <- createMemberInDB(group, googleMember)
    } yield (group, googleMember, created)

  }

  private def createMemberInDB(group: Group, member: Member)(implicit db: JdbcProfile#Backend#Database, ec: ExecutionContext): Future[Int] = {
    val NewMember = GoogleGroupToUserRow(
      group.id.get,
      member.id.get,
      member.email,
      "Y",
      member.role,
      member.memberType
    )
    db.run(googleGroupToUser += NewMember)
  }

  private def createGoogleMember(group: Group, member: Member)(implicit directory: Directory, ec: ExecutionContext)
  : Future[Member] = Future {
    directory.members.add(group.email, member)
  } recoverWith {
    case limitCap : GoogleJsonResponseException if limitCap.getLocalizedMessage.contains("exceeds") =>
      Thread.sleep(1000)
      createGoogleMember(group, member)
  }

  private def checkIfMemberExists(group: Group, member: Member)(implicit db: JdbcProfile#Backend#Database): Future[Boolean] = {

    val memberFilter = googleGroupToUser.withFilter(_.userEmail === member.email)
    val groupFilter = googleGroups.withFilter(_.email === group.email)

    val tableJoin = memberFilter join groupFilter on (_.groupId === _.id)
    val action = tableJoin.exists.result

    db.run(action)
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





}
