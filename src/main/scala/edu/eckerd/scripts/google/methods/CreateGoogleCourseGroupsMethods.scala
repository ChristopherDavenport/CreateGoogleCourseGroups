package edu.eckerd.scripts.google.methods

import slick.driver.JdbcProfile
import concurrent.Future
/**
  * Created by davenpcm on 6/30/16.
  */
trait CreateGoogleCourseGroupsMethods {
  profile: JdbcProfile =>
  import profile.api._


  case class ClassGroupMember(
                             courseName: String,
                             courseEmail: String,
                             studentEmail: String,
                             professorEmail: String,
                             term: String
                             )


  def getTermGroupInformationFromDatabase(implicit db: Database): Future[Seq[ClassGroupMember]] = {
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
ORDER BY alias asc;
    """.as[ClassGroupMember]

    db.run(data)
  }

  


}
