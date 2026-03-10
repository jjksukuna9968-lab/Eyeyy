## 
```
SELECT 
	CONCAT(ds.first_name, ' ', ds.middle_name, ' ', ds.last_name) AS student_name,
	ds.course_name,
	STRING_AGG(q.another_company_name, ', ') AS another_company_name,
	q.another_offer,
	q.this_offer
FROM (
	SELECT
		fact_metrics.student_id,
		dim_target_company.company_name		AS another_company_name,
		dim_job_offer.ctc 	 				AS another_offer,
		this_offer.ctc		 				AS this_offer,
		CASE WHEN dim_job_offer.ctc {operator} this_offer.ctc
			 THEN RANK() OVER (PARTITION BY fact_metrics.student_id ORDER BY dim_job_offer.ctc DESC)
			 ELSE NULL 
		END AS r
	FROM reporting_deliver.fact_college_metrics fact_metrics
	LEFT JOIN reporting_deliver.dim_target_company dim_target_company
	ON dim_target_company.target_company_id = fact_metrics.target_company_id
	LEFT JOIN reporting_deliver.dim_job_offer dim_job_offer
	ON dim_job_offer.offer_job_id = fact_metrics.offer_job_id 
	LEFT JOIN (
		SELECT 
			student_id,
			dim_target_company.company_id,
			max(ctc) AS ctc
		FROM reporting_deliver.dim_job_offer dim_job_offer
		LEFT JOIN reporting_deliver.dim_target_company dim_target_company
		ON dim_job_offer.company_id = dim_target_company.target_company_id
		WHERE dim_job_offer.college_id IN ({college_ids})
			AND dim_target_company.company_id IN ({company_ids})
		GROUP BY student_id,
		dim_target_company.company_id
	) AS this_offer
	ON this_offer.student_id = fact_metrics.student_id 
	{joins_clause} 
	WHERE {where_clause}
		AND dim_target_company.company_id NOT IN ({company_ids})
		AND dim_job_offer.ctc {operator} this_offer.ctc
) AS q
LEFT JOIN reporting_deliver.dim_student ds 
ON ds.student_id = q.student_id 
WHERE q.r = 1
GROUP BY CONCAT(ds.first_name, ' ', ds.middle_name, ' ', ds.last_name),
	ds.course_name,
	q.another_offer,
	q.this_offer
ORDER BY CONCAT(ds.first_name, ' ', ds.middle_name, ' ', ds.last_name) ASC;
```

## get_selected_students_list.sql

```
SELECT row_number() OVER (ORDER BY dtc.company_name, COALESCE(ac.first_name, ss.first_name)) AS serial_no,
       dtc.company_name                                                              AS company,
       jl.job_location,
       row_number() OVER (PARTITION BY dtc.company_id ORDER BY COALESCE(ac.first_name, ss.first_name)) AS sno,
       COALESCE(ac.first_name, ss.first_name)                                        AS first_name,
       COALESCE(ac.middle_name, ss.middle_name)                                      AS first_name,
       COALESCE(ac.last_name, ss.last_name)                                          AS last_name,
       fcm.degree_name                                                               AS degree,
       fcm.specialization_name                                                       AS branch,
       djo.ctc,
       COALESCE(ac.email, ss.email)                                                  AS email,
       ss.phone_number,
       dr.college_score,
       count(fcm.student_id) OVER (PARTITION BY dtc.company_id)                      AS placed_count
FROM (
  SELECT *
  FROM reporting_deliver.fact_college_metrics fcm
  WHERE college_id = {college_id}
        AND offer_job_id IS NOT NULL
        AND active
        {filters}
  ) fcm
LEFT JOIN reporting_deliver.dim_target_company dtc
  ON fcm.target_company_id = dtc.target_company_id
LEFT JOIN reporting_deliver.dim_job_offer djo
  ON fcm.offer_job_id = djo.offer_job_id
LEFT JOIN (
    SELECT pja.job_id,
           string_agg(city.name || ', ' || state.name, ' ; ') AS job_location
    FROM placements_jobaddress pja
    LEFT JOIN addresses_city city
      ON city.id = pja.city_id
    LEFT JOIN addresses_state state
      ON state.id = pja.state_id
    GROUP BY pja.job_id
) jl
  ON jl.job_id = djo.base_job_id
LEFT JOIN students_student ss
  ON fcm.student_id = ss.id
LEFT JOIN accounts_communityuser acu
  ON ss.community_user_id = acu.id
LEFT JOIN accounts_customuser ac
  ON acu.user_id = ac.id
LEFT JOIN (
  SELECT *
  FROM (
    SELECT rc.resume_id,
           rc.score         AS college_score,
           ROW_NUMBER() OVER (PARTITION BY rc.resume_id ORDER BY rc.is_current DESC, rc.updated_at DESC) AS row_num
    FROM resumes_resumecollege rc
  ) rc
  WHERE row_num = 1
) dr
  ON fcm.resume_id = dr.resume_id
ORDER BY serial_no
```


## get visited_companies_list.sql

```
-- placement_drive_ctc is temporary solution. We will add company_ctc column in fact_table
WITH placement_drive_ctc AS (
    SELECT
        pdj.placement_drive_id,
        pd.company_id AS target_company_id,
        max(pj.ctc)   AS ctc
    FROM placements_placementdrivejob AS pdj
    LEFT JOIN placements_placementdrive AS pd
        ON pdj.placement_drive_id = pd.id
    LEFT JOIN placements_job AS pj
        ON pdj.job_id = pj.basejob_ptr_id
    GROUP BY pdj.placement_drive_id,
        pd.company_id
)
SELECT
     row_number() OVER ()                                AS sno,
     upper(dtc.company_name)                             AS company,
       min(pp.event_datetime)::date                      AS campus_date,
       fcm.degree_name,
       string_agg(DISTINCT fcm.specialization_name, ' / ') AS branch,
       pd.eligibility_criteria,
       count(DISTINCT CASE WHEN fcm.offer_job_id IS NOT NULL THEN fcm.student_id END) AS students_selected,
       max(pdc.ctc)                                        AS ctc
FROM reporting_deliver.fact_college_metrics fcm
LEFT JOIN reporting_deliver.dim_placement_drive pd
    ON fcm.placement_drive_id = pd.placement_drive_id
LEFT JOIN placement_drive_ctc pdc
  ON pdc.placement_drive_id = fcm.placement_drive_id
    AND pdc.target_company_id = fcm.target_company_id
LEFT JOIN reporting_deliver.dim_target_company dtc
  ON fcm.target_company_id = dtc.target_company_id
LEFT JOIN placements_placementdriveschedule pp
  ON fcm.placement_drive_id = pp.placement_drive_id
WHERE fcm.college_id = {college_id}
  AND fcm.degree_name IS NOT NULL
  AND dtc.company_name IS NOT NULL
  AND fcm.active
  {filters}
GROUP BY upper(dtc.company_name), fcm.degree_name, pd.eligibility_criteria
```

## placement_stats

```
WITH placement_drive_comp AS (
    SELECT
        pdj.placement_drive_id,
        pd.company_id AS target_company_id,
        max(coalesce(pj.{sal}, 0))   AS {sal}
    FROM placements_placementdrive{type} AS pdj
    LEFT JOIN placements_placementdrive AS pd
        ON pdj.placement_drive_id = pd.id
    LEFT JOIN placements_{type} AS pj
        ON pdj.{type}_id = pj.basejob_ptr_id
    GROUP BY pdj.placement_drive_id,
        pd.company_id
)
SELECT dc.organisation_name         AS college_name,
       conv_to_drange(batch)        AS batch,
       pres_yr_companies_count,
       prev_yr_companies_count,
       pres_yr_companies_count - prev_yr_companies_count AS companies_count_increment,
       pres_yr_placement_drives_count,
       prev_yr_placement_drives_count,
       pres_yr_placement_drives_count - prev_yr_placement_drives_count AS placement_drives_increment,
       comp_below_{sal_cutoff_1},
       comp_btw_{sal_cutoff_1}_{sal_cutoff_2},
       comp_above_{sal_cutoff_2},
       pres_yr_offers,
       prev_yr_offers,
       pres_yr_offers - prev_yr_offers      AS offers_count_increment,
       ctc_below_{sal_cutoff_1},
       ctc_btw_{sal_cutoff_1}_{sal_cutoff_2},
       ctc_above_{sal_cutoff_2}
FROM (
  SELECT college_id,
         batch,
         total_companies  AS pres_yr_companies_count,
         coalesce(lag(total_companies) OVER (PARTITION BY college_id ORDER BY batch), 0)   AS prev_yr_companies_count,
         total_drives     AS pres_yr_placement_drives_count,
         coalesce(lag(total_drives) OVER(PARTITION BY college_id ORDER BY batch), 0) AS prev_yr_placement_drives_count,
         total_offers     AS pres_yr_offers,
         coalesce(lag(total_offers) OVER(PARTITION BY college_id ORDER BY batch), 0) AS prev_yr_offers,
         ctc_below_{sal_cutoff_1},
         ctc_btw_{sal_cutoff_1}_{sal_cutoff_2},
         ctc_above_{sal_cutoff_2},
         comp_below_{sal_cutoff_1},
         comp_btw_{sal_cutoff_1}_{sal_cutoff_2},
         comp_above_{sal_cutoff_2}
  FROM (
    SELECT fcm.college_id,
           count(DISTINCT fcm.placement_drive_id) AS total_drives,
           count(DISTINCT dtc.company_id)         AS total_companies,
           count(DISTINCT fcm.offer_{type}_id)       AS total_offers,
           count(DISTINCT CASE WHEN djio.{sal} < {sal_cutoff_1} THEN fcm.offer_{type}_id END) AS ctc_below_{sal_cutoff_1},
           count(DISTINCT CASE WHEN djio.{sal} >= {sal_cutoff_1} AND djio.{sal} <= {sal_cutoff_2} THEN fcm.offer_{type}_id END) AS ctc_btw_{sal_cutoff_1}_{sal_cutoff_2},
           count(DISTINCT CASE WHEN djio.{sal} > {sal_cutoff_2} THEN fcm.offer_{type}_id END) AS ctc_above_{sal_cutoff_2},
           count(DISTINCT CASE WHEN coalesce(pdc.{sal}, 0) < {sal_cutoff_1} THEN dtc.company_id END) AS comp_below_{sal_cutoff_1},
           count(DISTINCT CASE WHEN coalesce(pdc.{sal}, 0) >= {sal_cutoff_1} AND coalesce(pdc.{sal}, 0) <= {sal_cutoff_2} THEN dtc.company_id END) AS comp_btw_{sal_cutoff_1}_{sal_cutoff_2},
           count(DISTINCT CASE WHEN coalesce(pdc.{sal}, 0) > {sal_cutoff_2} THEN dtc.company_id END) AS comp_above_{sal_cutoff_2},
           split_into_years(coalesce(pds.batch, fcm.last_updated_at)::date)     AS batch
    FROM (
        SELECT DISTINCT placement_drive_id,
               college_id,
               target_company_id,
               offer_job_id,
               offer_internship_id,
               last_updated_at
        FROM reporting_deliver.fact_college_metrics
        {filters}
    ) fcm
    LEFT JOIN (
        SELECT pds.placement_drive_id,
               min(coalesce(pds.event_datetime, pd.created_at)::date) AS batch
        FROM public.placements_placementdrive pd
        LEFT JOIN public.placements_placementdriveschedule pds
          ON pd.id = pds.placement_drive_id
        GROUP BY pds.placement_drive_id
    ) pds
      ON fcm.placement_drive_id = pds.placement_drive_id
    LEFT JOIN placement_drive_comp pdc
      ON pdc.placement_drive_id = fcm.placement_drive_id
          AND pdc.target_company_id = fcm.target_company_id
    LEFT JOIN reporting_deliver.dim_target_company dtc
      ON fcm.target_company_id = dtc.target_company_id
    LEFT JOIN reporting_deliver.dim_{type}_offer{s} djio
      ON fcm.offer_{type}_id = djio.offer_{type}_id
    WHERE (fcm.placement_drive_id IS NOT NULL
        OR fcm.offer_job_id IS NOT NULL)
        AND pds.batch <= CURRENT_DATE::date
    GROUP BY fcm.college_id, split_into_years(coalesce(pds.batch, fcm.last_updated_at)::date)
  ) fcm
  WHERE batch IS NOT NULL
) college_stats
LEFT JOIN reporting_deliver.dim_colleges dc
  ON college_stats.college_id = dc.college_id
{batch_filters}
ORDER BY dc.organisation_name, batch
```

## get_placements_and_higher_studies_details.sql

```
SELECT
  CONCAT({academic_year}, '-', {academic_year} + 1)                                                              AS academic_year,
  CONCAT(CASE
           WHEN course.level = 1
             THEN 'Diploma'
           WHEN course.level = 2
             THEN 'Graduate'
           WHEN course.level = 3
             THEN 'Post Graduate'
           WHEN course.level = 7
             THEN 'Doctorate'
           WHEN course.level = 8
             THEN 'Post Graduate Diploma'
           WHEN course.level = 6
             THEN 'Others'
           WHEN course.level = 5
             THEN 'Dual' END, '  [', course.course_years,
         'Years]')                                                                                               AS program_level,
  COUNT(DISTINCT (fact.student_id))
  FILTER (WHERE fact.offer_job_id IS NOT NULL)                                                                   AS students_placed,
  NULL                                                                                                           AS students_for_higher_studies,
  COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY job.ctc) FILTER (WHERE fact.offer_job_id IS NOT NULL), 0) AS median_salary
FROM (
  SELECT
    student_id,
    offer_job_id,
    course_id,
    college_id
  FROM reporting_deliver.fact_college_metrics
  WHERE college_id = {college_id}
    AND graduation_year = {academic_year}
    AND course_id IS NOT NULL
) AS fact
LEFT JOIN reporting_deliver.dim_course course
  ON fact.course_id = course.course_id
LEFT JOIN reporting_deliver.dim_job_offer job
  ON fact.offer_job_id = job.offer_job_id
GROUP BY
  course.level,
  course.course_years
```


## placement_stats.sql

```
 SELECT 
    hiring_stats.college_name,
    hiring_stats.company_name,
    hiring_stats.target_batch,
    hiring_stats.drive_title,
    hiring_stats.no_of_students_applied,
    hiring_stats.no_of_students_offered,
    hiring_stats.course_degree_name,
    job.job_positions,
    COALESCE(job.ctc, hiring_stats.max_ctc_offered) AS ctc_offered,
    hiring_stats.drive_date
FROM (
SELECT 
    col.organisation_name                                                        AS college_name,
    company.target_company_id,
    company.company_name,
    company.target_batch,
    count(DISTINCT fact.student_id)                                              AS no_of_students_applied,
    count(DISTINCT fact.student_id) FILTER (WHERE fact.offer_job_id IS NOT NULL) AS no_of_students_offered,
    string_agg(DISTINCT course.course_name , ', ')                               AS course_degree_name,
    max(job.ctc)                                                                 AS max_ctc_offered,
    string_agg(DISTINCT pd.title, ', ') FILTER (WHERE pd.title IS NOT NULL )     AS drive_title,
    max(pd.pd_first_scheduled_at)                                                AS drive_date
FROM (
    SELECT college_id, 
        target_company_id, 
        placement_drive_id, 
        offer_job_id, 
        student_id , 
        graduation_year,
        course_id
    FROM reporting_deliver.fact_college_metrics fcm 
    WHERE COALESCE(placement_drive_type, -1) != 3
        AND COALESCE(placement_drive_status, -1) != 10
        AND COALESCE(student_status, -1) NOT IN (7, 8, 9, 12)
        AND fcm.active
        AND placement_drive_id IS NOT NULL
        AND is_dummy_college IS FALSE
    ) AS fact
LEFT JOIN reporting_deliver.dim_course course 
    ON fact.course_id = course.course_id 
LEFT JOIN reporting_deliver.dim_target_company company
    ON fact.target_company_id = company.target_company_id
LEFT JOIN reporting_deliver.dim_colleges col
    ON fact.college_id = col.college_id
LEFT JOIN reporting_deliver.dim_job_offer job 
    ON fact.offer_job_id = job.offer_job_id
LEFT JOIN reporting_deliver.dim_placement_drive pd
    ON fact.placement_drive_id = pd.placement_drive_id
GROUP BY col.organisation_name, 
    company.target_company_id,
    company.company_name,
    company.target_batch 
) AS hiring_stats
LEFT JOIN (
    SELECT target_company_id, 
        string_agg(DISTINCT title, ',') AS job_positions, 
        max(ctc)                        AS ctc
    FROM placements_basejob bj
    JOIN placements_job j
        ON bj.id = j.basejob_ptr_id
    GROUP BY target_company_id
) AS job 
    ON hiring_stats.target_company_id = job.target_company_id;
```


## assessment_reports.sql

```
SELECT
    ui.first_name,
    ui.middle_name,
    ui.last_name,
    student_details.graduation_year                  AS batch,
    ui.course,
    org.name                                         AS college_name,
    student_details.tenth_score,
    student_details.twelfth_score,
    student_details.college_score,
    student_details.normalized_school10_score,
    student_details.normalized_school12_score,
    student_details.normalized_college_score,
    student_details.score                            AS calyx_score,
    (reports.marks_obtained / a.maximum_marks) * 100 AS assessment_score,
    reports.very_easy_and_correct,
    reports.very_easy_and_attempted,
    reports.easy_and_correct,
    reports.easy_and_attempted,
    reports.moderate_and_correct,
    reports.moderate_and_attempted,
    reports.difficult_and_correct,
    reports.difficult_and_attempted,
    reports.very_difficult_and_correct,
    reports.very_difficult_and_attempted,
    student_details.ctc,
    student_details.stipend,
    student_details.college_weight
FROM (
    SELECT
        assessment_id,
        user_assessment_id,
        user_info_id,
        sum(marks_obtained)                                                             AS marks_obtained,
        sum(CASE WHEN difficulty_level = 1 AND evaluation_status = 1 THEN 1 ELSE 0 END) AS very_easy_and_correct,
        sum(CASE WHEN difficulty_level = 1 AND evaluation_status IS NOT NULL THEN 1 ELSE 0 END ) AS very_easy_and_attempted,
        sum(CASE WHEN difficulty_level = 2 AND evaluation_status = 1 THEN 1 ELSE 0 END) AS easy_and_correct,
        sum(CASE WHEN difficulty_level = 2 AND evaluation_status IS NOT NULL THEN 1 ELSE 0 END ) AS easy_and_attempted,
        sum(CASE WHEN difficulty_level = 3 AND evaluation_status = 1 THEN 1 ELSE 0 END) AS moderate_and_correct,
        sum(CASE WHEN difficulty_level = 3 AND evaluation_status IS NOT NULL THEN 1 ELSE 0 END ) AS moderate_and_attempted,
        sum(CASE WHEN difficulty_level = 4 AND evaluation_status = 1 THEN 1 ELSE 0 END) AS difficult_and_correct,
        sum(CASE WHEN difficulty_level = 4 AND evaluation_status IS NOT NULL THEN 1 ELSE 0 END ) AS difficult_and_attempted,
        sum(CASE WHEN difficulty_level = 5 AND evaluation_status = 1 THEN 1 ELSE 0 END) AS very_difficult_and_correct,
        sum(CASE WHEN difficulty_level = 5 AND evaluation_status IS NOT NULL THEN 1 ELSE 0 END ) AS very_difficult_and_attempted,
        organisation_id
    FROM reporting_deliver.fact_user_question_response fuqr
    WHERE assessment_id = '{assessment_id}' AND is_group_question IS FALSE
    GROUP BY organisation_id, assessment_id, user_assessment_id, user_info_id
) AS reports
LEFT JOIN reporting_deliver.dim_assessment a
    ON reports.assessment_id = a.assessment_id
LEFT JOIN reporting_deliver.dim_user_info ui
    ON reports.user_info_id = ui.user_info_id
LEFT JOIN reporting_deliver.dim_organisation org
    ON reports.organisation_id = org.organisation_id AND is_deleted is FALSE
LEFT JOIN (
    SELECT student.email,
        student.graduation_year,
        resume.tenth_score,
        resume.twelfth_score,
        resume.college_score,
        ss.school10_score          AS normalized_school10_score,
        ss.school12_score          AS normalized_school12_score,
        ss.college_aggregate_score AS normalized_college_score,
        ss.student_score           AS score,
        col.third_party_org_id,
        fact.ctc,
        fact.stipend,
        cw.weighted_score          AS college_weight
    FROM (
        SELECT
            fact.student_id,
            fact.resume_id,
            fact.college_id,
            max(job.ctc)            AS ctc,
            max(internship.stipend) AS stipend
        FROM reporting_deliver.fact_college_metrics fact
        LEFT JOIN reporting_deliver.dim_job_offer job
	        ON fact.offer_job_id = job.offer_job_id
        LEFT JOIN reporting_deliver.dim_internship_offers internship
	        ON fact.offer_internship_id = internship.offer_internship_id
        GROUP BY fact.student_id, fact.resume_id, fact.college_id
    ) AS fact
    LEFT JOIN reporting_deliver.dim_student student
        ON fact.student_id = student.student_id
    LEFT JOIN reporting_deliver.dim_colleges col
        ON fact.college_id = col.college_id
    LEFT JOIN reporting_deliver.college_weights cw
        ON col.name = cw.college_name
    LEFT JOIN reporting_deliver.dim_resume resume
        ON fact.resume_id = resume.resume_id
    LEFT JOIN reporting_clean.student_scores ss
        ON fact.student_id = ss.student_id
) AS student_details
    ON ui.email = student_details.email
        AND org.third_party_org_id = student_details.third_party_org_id
ORDER BY
  ui.first_name,
  ui.middle_name,
  ui.last_name
```

## assessment_reports.sql

```
SELECT
    ui.first_name,
    ui.middle_name,
    ui.last_name,
    student_details.graduation_year                  AS batch,
    ui.course,
    org.name                                         AS college_name,
    student_details.tenth_score,
    student_details.twelfth_score,
    student_details.college_score,
    student_details.normalized_school10_score,
    student_details.normalized_school12_score,
    student_details.normalized_college_score,
    student_details.score                            AS calyx_score,
    (reports.marks_obtained / a.maximum_marks) * 100 AS assessment_score,
    reports.very_easy_and_correct,
    reports.very_easy_and_attempted,
    reports.easy_and_correct,
    reports.easy_and_attempted,
    reports.moderate_and_correct,
    reports.moderate_and_attempted,
    reports.difficult_and_correct,
    reports.difficult_and_attempted,
    reports.very_difficult_and_correct,
    reports.very_difficult_and_attempted,
    student_details.ctc,
    student_details.stipend,
    student_details.college_weight
FROM (
    SELECT
        assessment_id,
        user_assessment_id,
        user_info_id,
        sum(marks_obtained)                                                             AS marks_obtained,
        sum(CASE WHEN difficulty_level = 1 AND evaluation_status = 1 THEN 1 ELSE 0 END) AS very_easy_and_correct,
        sum(CASE WHEN difficulty_level = 1 AND evaluation_status IS NOT NULL THEN 1 ELSE 0 END ) AS very_easy_and_attempted,
        sum(CASE WHEN difficulty_level = 2 AND evaluation_status = 1 THEN 1 ELSE 0 END) AS easy_and_correct,
        sum(CASE WHEN difficulty_level = 2 AND evaluation_status IS NOT NULL THEN 1 ELSE 0 END ) AS easy_and_attempted,
        sum(CASE WHEN difficulty_level = 3 AND evaluation_status = 1 THEN 1 ELSE 0 END) AS moderate_and_correct,
        sum(CASE WHEN difficulty_level = 3 AND evaluation_status IS NOT NULL THEN 1 ELSE 0 END ) AS moderate_and_attempted,
        sum(CASE WHEN difficulty_level = 4 AND evaluation_status = 1 THEN 1 ELSE 0 END) AS difficult_and_correct,
        sum(CASE WHEN difficulty_level = 4 AND evaluation_status IS NOT NULL THEN 1 ELSE 0 END ) AS difficult_and_attempted,
        sum(CASE WHEN difficulty_level = 5 AND evaluation_status = 1 THEN 1 ELSE 0 END) AS very_difficult_and_correct,
        sum(CASE WHEN difficulty_level = 5 AND evaluation_status IS NOT NULL THEN 1 ELSE 0 END ) AS very_difficult_and_attempted,
        organisation_id
    FROM reporting_deliver.fact_user_question_response fuqr
    WHERE assessment_id = '{assessment_id}' AND is_group_question IS FALSE
    GROUP BY organisation_id, assessment_id, user_assessment_id, user_info_id
) AS reports
LEFT JOIN reporting_deliver.dim_assessment a
    ON reports.assessment_id = a.assessment_id
LEFT JOIN reporting_deliver.dim_user_info ui
    ON reports.user_info_id = ui.user_info_id
LEFT JOIN reporting_deliver.dim_organisation org
    ON reports.organisation_id = org.organisation_id AND is_deleted is FALSE
LEFT JOIN (
    SELECT student.email,
        student.graduation_year,
        resume.tenth_score,
        resume.twelfth_score,
        resume.college_score,
        ss.school10_score          AS normalized_school10_score,
        ss.school12_score          AS normalized_school12_score,
        ss.college_aggregate_score AS normalized_college_score,
        ss.student_score           AS score,
        col.third_party_org_id,
        fact.ctc,
        fact.stipend,
        cw.weighted_score          AS college_weight
    FROM (
        SELECT
            fact.student_id,
            fact.resume_id,
            fact.college_id,
            max(job.ctc)            AS ctc,
            max(internship.stipend) AS stipend
        FROM reporting_deliver.fact_college_metrics fact
        LEFT JOIN reporting_deliver.dim_job_offer job
	        ON fact.offer_job_id = job.offer_job_id
        LEFT JOIN reporting_deliver.dim_internship_offers internship
	        ON fact.offer_internship_id = internship.offer_internship_id
        GROUP BY fact.student_id, fact.resume_id, fact.college_id
    ) AS fact
    LEFT JOIN reporting_deliver.dim_student student
        ON fact.student_id = student.student_id
    LEFT JOIN reporting_deliver.dim_colleges col
        ON fact.college_id = col.college_id
    LEFT JOIN reporting_deliver.college_weights cw
        ON col.name = cw.college_name
    LEFT JOIN reporting_deliver.dim_resume resume
        ON fact.resume_id = resume.resume_id
    LEFT JOIN reporting_clean.student_scores ss
        ON fact.student_id = ss.student_id
) AS student_details
    ON ui.email = student_details.email
        AND org.third_party_org_id = student_details.third_party_org_id
ORDER BY
  ui.first_name,
  ui.middle_name,
  ui.last_name
```

## company_level_data.sql
```
SELECT *
FROM (
  SELECT fcm.company_id,
         fcm.candidate_application_id,
         fcm.recruitment_drive_id,
         ca.first_name,
         ca.middle_name,
         ca.last_name,
         ca.phone_number,
         ca.email,
         dca.marital_status,
         dca.known_languages,
         ca.blood_group,
         dca.referred,
         ca.current_experience_years,
         ca.current_experience_months,
         ca.current_ctc,
         ca.total_experience_years,
         ca.total_experience_months,
         dca.is_fresher,
         dca.college_name,
         -- university name logic has changed since DS-946. needs to be updated here
        --  dca.university_name,
         fcm.degree,
         fcm.stream,
         dca.gender,
         dca.dob,
         COALESCE(dca.batch, fcm.batch)     AS batch,
         dca.graduation_score,
         fcm.college_aggregate_score,
         dca.twelfth_score,
         fcm.school12_score,
         dca.twelfth_board_name,
         dca.tenth_score,
         fcm.school10_score,
         dca.tenth_board_name,
         fcm.calyx_score           AS candidate_score,
         dr.project_skills,
         dr.cert_skills,
         dr.research_skills,
         dr.intern_skills,
         dr.workex_skills,
         dr.training_skills,
         dr.resume_skills,
         dca.permanent_city,
         dca.permanent_state,
         dca.current_city,
         dca.current_state,
         dca.source_type,
         dca.candidate_source,
         fcm.interviewer_names,
         fcm.interview_time_spent_secs,
         fcm.number_of_interviewers,
         fcm.title,
         djo.ctc,
         fcm.joining_status,
         fcm.stage_status[1]          AS stage_status,
         fcm.is_absent,
         fcm.recruitment_undergoing,
         fcm.is_round_passed,
         fcm.is_offered,
         CASE
            WHEN fcm.is_offered IS FALSE
                AND fcm.recruitment_undergoing IS TRUE
                THEN NULL
            WHEN fcm.is_offered IS FALSE
                AND fcm.is_absent IS TRUE
                THEN -1
            WHEN fcm.is_offered IS TRUE
                THEN 1
            ELSE 0
         END                    AS new_is_offered
  FROM (
    SELECT fcm.company_id,
         fcm.candidate_application_id,
         fcm.recruitment_drive_id,
         fcm.resume_id,
         fcm.offer_job_id,
         fcm.graduation_year                                AS batch,
         fcm.degree,
         fcm.stream,
         fcm.college_aggregate_score,
         fcm.calyx_score,
         fcm.school10_score,
         fcm.school12_score,
         array_remove(ARRAY_AGG(DISTINCT ips.stage_status), NULL)    AS stage_status,
         SUM(COALESCE(fcm.interview_time_spent_secs, 0))     AS interview_time_spent_secs,
         ARRAY_AGG(DISTINCT dcu.name)                        AS interviewer_names,
         COUNT(DISTINCT dcu.name)                            AS number_of_interviewers,
         ARRAY_AGG(DISTINCT dp.title)                        AS title,
         fcm.joining_status, 
         bool_or(fcm.recruitment_undergoing)                 AS recruitment_undergoing,
         bool_or(fcm.is_absent)                              AS is_absent,
         bool_or(COALESCE(ips.new_is_round_passed, FALSE))   AS is_round_passed,
         bool_or(fcm.is_offered)                             AS is_offered
    FROM (
          SELECT *,
                 CASE
                    WHEN fcm.is_round_passed = 'absent'
                      THEN TRUE
                    ELSE FALSE
                 END                                      AS is_absent,
                 CASE
                    WHEN fcm.ds_process_type IN ('offered')
                        AND fcm.is_round_passed = 'selected'
                    THEN TRUE
                    ELSE FALSE
                 END                                      AS is_offered,
                 CASE
                    WHEN fcm.next_candidate_stage IS NULL
                        AND fcm.ds_process_type NOT IN ('applied', 'shortlisted', 'application_not_selected', 'absent',
                              'selected', 'not_selected', 'on_hold', 'process_completed',
                              'offered', 'closures', 'non_closures')
                        THEN TRUE
                    ELSE FALSE
                 END                                      AS recruitment_undergoing
          FROM reporting_deliver.fact_companymetrics fcm
          ) fcm
    LEFT JOIN reporting_deliver.dim_companyuser dcu
        ON fcm.interviewer_community_user_id = dcu.id
    LEFT JOIN reporting_deliver.dim_recruitmentdrive AS dp
        ON fcm.recruitment_drive_id = dp.id
    LEFT JOIN reporting_deliver.dim_stages AS ds
        ON fcm.stage_id = ds.id
    LEFT JOIN (
        SELECT *,
               TRUE     AS new_is_round_passed,
               ROW_NUMBER() OVER (PARTITION BY company_id, recruitment_drive_id, candidate_application_id ORDER BY process_stage_order) AS ranks
        FROM (
          SELECT fcm.company_id,
                 fcm.recruitment_drive_id,
                 fcm.candidate_application_id,
                 fcm.stage_id,
                 fcm.title,
                 fcm.process_stage_order,
                 fcm.custom_process_type,
                 fcm.previous_stage_id,
                 rc.status              AS stage_status
          FROM (
            SELECT *,
            lag(stage_id) OVER (PARTITION BY company_id, recruitment_drive_id, candidate_application_id ORDER BY process_stage_order) AS previous_stage_id
            FROM (
                SELECT DISTINCT 
                    fcm.company_id,
                    fcm.candidate_application_id,
                    fcm.recruitment_drive_id,
                    fcm.stage_id,
                    rdps.title,
                    rdps.process_stage_order,
                    rdps.custom_process_type
                FROM reporting_deliver.fact_companymetrics fcm
                  LEFT JOIN recruitments_recruitmentdriveprocessstage rdps
                    ON fcm.stage_id = rdps.id
                WHERE rdps.process_stage_order NOT IN (-30, -20, -10, 990, 1000, 1010, 1020, 1025, 1030, 1040, 1050)
                OR (fcm.ds_process_type IN ('applied', 'shortlisted', 'selected', 'offered', 'closures', 'non_closures'))
            ) fcm
          ) fcm
          LEFT JOIN recruitments_recruitmentdrivecandidate rdc
            ON fcm.candidate_application_id = rdc.candidate_application_id
              AND fcm.recruitment_drive_id = rdc.recruitment_drive_id
          LEFT JOIN recruitments_candidaterecruitmentdriveprocessstage rc
            ON rdc.id = rc.candidate_id
              AND rc.recruitment_drive_process_stage_id = fcm.stage_id
          WHERE title NOT IN ('applied', 'shortlisted', 'application_not_selected', 'absent',
                              'selected', 'not_selected', 'on_hold', 'process_completed',
                              'offered', 'closures', 'non_closures')
                AND custom_process_type = 1
        ) f
      ) ips
        ON ips.company_id = fcm.company_id
          AND ips.recruitment_drive_id = fcm.recruitment_drive_id
          AND ips.candidate_application_id = fcm.candidate_application_id
          AND ips.previous_stage_id = fcm.stage_id
          AND ips.ranks = 1
    GROUP BY fcm.company_id, fcm.candidate_application_id, fcm.recruitment_drive_id, fcm.resume_id, fcm.graduation_year,
          fcm."degree", fcm.stream, fcm.offer_job_id, fcm.joining_status, fcm.college_aggregate_score,
          fcm.calyx_score, fcm.school10_score, fcm.school12_score
  ) fcm
  LEFT JOIN candidates_candidateapplication AS ca
    ON fcm.candidate_application_id = ca.id
  LEFT JOIN reporting_deliver.dim_candidate_application AS dca
      ON fcm.candidate_application_id = dca.id
  LEFT JOIN reporting_deliver.dim_resume dr
      ON fcm.resume_id = dr.resume_id
  LEFT JOIN reporting_deliver.dim_job_offer djo
      ON djo.offer_job_id = fcm.offer_job_id
) chd
```


## jtg_round_wise_customerscore_data_fe.sql

```
WITH scores AS (
  SELECT swr.candidate_application_id,
               swr.candidate_id,
               swr.recruitment_drive_id,
               (ARRAY_AGG(swr.mcq_score) FILTER (WHERE swr.mcq_score IS NOT NULL))[1]         AS mcq_score,
               (ARRAY_AGG(swr.written_score) FILTER (WHERE swr.written_score IS NOT NULL))[1] AS written_score
        FROM (
          SELECT candidate_application_id,
                 candidate_id,
                 recruitment_drive_id,
                 stage_id,
                 custom_process_type,
                 score_type,
                 score_label,
                 process_order,
                 CASE
                    WHEN score_label IN ('MCQ',
                                      'FE Freshers- 2021 Batch- November- Day 1 - C/C++ MCQs',
                                      'FE Freshers- Objective- 2021 Batch- November- Day 2 - C/C++ MCQs',
                                      'FE Freshers- Objective- 2021 Batch- November- Day 3 - C/C++ MCQs',
                                      'FE Freshers- Objective- 2021 Batch- November- Day 4 - C/C++ MCQs',
                                      'FE MCQ',
                                      'Total',
                                      'Only Front End - Objective - 1',
                                      'FE+SD Objective 2021 - 1',
                                      'Only Front End - Objective - 2',
                                      'FE Freshers - Interns (MCQs) - 2021 Batch - C/C++ MCQs',
                                      'FE Freshers - Interns (New) - Day 2 - FE MCQs',
                                      'FE Freshers - Interns (New) - Day 3 - FE MCQs',
                                      'FE Freshers - Interns (New) - Day 4 - FE MCQs',
                                      'FE Freshers - Interns (New) - FE MCQs',
                                      'FE Freshers - Full Time (Batch 2) - MCQs',
                                      'FE Freshers - Full Time (New) - MCQs')
                           AND max_score > 10
                      THEN (score/max_score * 100)
                    ELSE NULL
                 END    AS mcq_score,
                 CASE
                    WHEN score_label IN ('Total',
                                      'FE Freshers- Subjective- 2021 Batch- November- Day 1 - Coding Questions',
                                      'FE Freshers- Subjective- 2021 Batch- November- Day 2 - Coding Questions',
                                      'FE Freshers- Subjective- 2021 Batch- November- Day 3 - Coding Questions',
                                      'FE Freshers- Subjective- 2021 Batch- November- Day 4 - Coding Questions',
                                      'FE Total',
                                      'Only Front End Subjective Test',
                                      'Only FE Subjective 2020 -1',
                                      'Only FE Subjective - 2021 - Set 1',
                                      'FE Freshers - Interns (New) - Coding Questions',
                                      'FE Freshers - Interns (New) - Day 2 - Coding Questions',
                                      'FE Freshers - Interns (New) - Day 3 - Coding Questions',
                                      'FE Freshers - Interns (New) - Day 4 - Coding Questions',
                                      'FE Freshers- Subjective- 2021 Batch - Coding Questions',
                                      'FE Freshers - Full Time (Batch 2) - Coding Questions',
                                      'FE Freshers - Full Time (New) - Coding Questions'
                    )
                      AND max_score <= 10
                      THEN (score/max_score * 100)
                    ELSE NULL
                 END    AS written_score
          FROM (
            SELECT rdc.candidate_application_id,
                   rc.candidate_id,
                   rdc.recruitment_drive_id,
                   rr.id                    AS stage_id,
                   rr.custom_process_type,
                   array_agg(rc.custom_scores) 
                          FILTER (WHERE rc.custom_scores IS NOT NULL)
                          OVER (PARTITION BY rdc.candidate_application_id, rdc.recruitment_drive_id)
                              AS custom_scores,
                   NULLIF(rc."value", '')::NUMERIC      AS score,
                   NULLIF(Json(rr.value)->>'type', '')  AS score_type,
                   NULLIF(Json(rr.value)->>'label', '') AS score_label,
                   NULLIF(Json(rr.value)->>'order', '') AS process_order,
                   NULLIF(Json(Json(rr.value)->>'validations')->>'max', '')::numeric AS max_score
            FROM recruitments_recruitmentdrivecandidate rdc
              LEFT JOIN (
                SELECT rc.id, candidate_id, recruitment_drive_process_stage_id, status,
                       (jsonb_each_text(custom_scores)).*,
                       rc.custom_scores,
                       ds.title,
                       ds.process_stage_order
                FROM recruitments_candidaterecruitmentdriveprocessstage rc
                LEFT JOIN reporting_deliver.dim_stages ds
                  ON rc.recruitment_drive_process_stage_id = ds.id
                WHERE rc.id NOT IN (1166646)
              ) rc
                ON rdc.id = rc.candidate_id
              LEFT JOIN (
                SELECT id,
                       recruitment_drive_id,
                       custom_process_type,
                       (jsonb_each_text(custom_fields_template)).*
                FROM recruitments_recruitmentdriveprocessstage rr
              ) rr
              ON rr."key" = rc."key"
                AND rr.recruitment_drive_id = rdc.recruitment_drive_id
                AND Json(rr.value)->>'type' = 'number'
          ) swr
          WHERE swr.custom_scores IS NOT NULL
              AND custom_process_type IN (3, 4)
          ORDER BY candidate_application_id, recruitment_drive_id, stage_id, process_order
        ) swr
        GROUP BY swr.candidate_application_id, swr.recruitment_drive_id, swr.candidate_id
)
SELECT *
FROM (
  SELECT fcm.company_id,
         fcm.candidate_application_id,
         fcm.candidate_id,
         fcm.recruitment_drive_id,
         ca.first_name,
         ca.middle_name,
         ca.last_name,
         ca.phone_number,
         ca.email,
         dca.marital_status,
         dca.known_languages,
         ca.blood_group,
         dca.referred,
         ca.current_experience_years,
         ca.current_experience_months,
         ca.current_ctc,
         ca.total_experience_years,
         ca.total_experience_months,
         dca.is_fresher,
         dca.college_name,
         -- university name logic has changed since DS-946. needs to be updated here
        --  dca.university_name,
         fcm.degree,
         fcm.stream,
         dca.gender,
         dca.dob,
         dca.batch,
         dca.graduation_score,
         fcm.college_aggregate_score,
         dca.twelfth_score,
         fcm.school12_score,
         dca.twelfth_board_name,
         dca.tenth_score,
         fcm.school10_score,
         dca.tenth_board_name,
         fcm.calyx_score           AS candidate_score,
         dr.project_skills,
         dr.cert_skills,
         dr.research_skills,
         dr.intern_skills,
         dr.workex_skills,
         dr.training_skills,
         dr.resume_skills,
         dca.permanent_city,
         dca.permanent_state,
         dca.current_city,
         dca.current_state,
         dca.source_type,
         dca.candidate_source,
         fcm.interviewer_names,
         fcm.interview_time_spent_secs,
         fcm.number_of_interviewers,
         fcm.title,
         fcm.departments,
         djo.ctc,
         fcm.joining_status,
         fcm.mcq_score,
         fcm.written_score,
         fcm.is_absent,
         fcm.recruitment_undergoing,
         fcm.is_round_passed,
         fcm.is_offered,
         CASE
            WHEN fcm.is_offered IS FALSE
                AND fcm.recruitment_undergoing IS TRUE
                THEN NULL
            WHEN fcm.is_offered IS FALSE
                AND fcm.is_absent IS TRUE
                THEN -1
            WHEN fcm.is_offered IS TRUE
                THEN 1
            ELSE 0
         END                    AS new_is_offered
  FROM (
    SELECT
         fcm.company_id,
         fcm.candidate_application_id,
         swr.candidate_id,
         fcm.recruitment_drive_id,
         fcm.resume_id,
         fcm.offer_job_id,
         fcm.degree,
         fcm.stream,
         fcm.college_aggregate_score,
         fcm.calyx_score,
         fcm.school10_score,
         fcm.school12_score,
         SUM(COALESCE(fcm.interview_time_spent_secs, 0))     AS interview_time_spent_secs,
         ARRAY_AGG(DISTINCT dcu.name)                        AS interviewer_names,
         COUNT(DISTINCT dcu.name)                            AS number_of_interviewers,
         ARRAY_AGG(DISTINCT dp.title)                        AS title,
         ARRAY_AGG(DISTINCT dcd.department_name)             AS departments,
         fcm.joining_status,
         bool_or(fcm.recruitment_undergoing)                 AS recruitment_undergoing,
         bool_or(fcm.is_absent)                              AS is_absent,
         bool_or(COALESCE(ips.new_is_round_passed, FALSE))   AS is_round_passed,
         bool_or(fcm.is_offered)                             AS is_offered,
         swr.written_score,
         swr.mcq_score
    FROM (
          SELECT *,
                 CASE
                    WHEN fcm.is_round_passed = 'absent'
                      THEN TRUE
                    ELSE FALSE
                 END                                      AS is_absent,
                 CASE
                    WHEN fcm.ds_process_type IN ('offered')
                        AND fcm.is_round_passed = 'selected'
                    THEN TRUE
                    ELSE FALSE
                 END                                      AS is_offered,
                 CASE
                    WHEN fcm.next_candidate_stage IS NULL
                        AND fcm.ds_process_type NOT IN ('applied', 'shortlisted', 'application_not_selected', 'absent',
                              'selected', 'not_selected', 'on_hold', 'process_completed',
                              'offered', 'closures', 'non_closures')
                        THEN TRUE
                    ELSE FALSE
                 END                                      AS recruitment_undergoing
          FROM reporting_deliver.fact_companymetrics fcm
          WHERE recruitment_drive_id NOT IN (10118) --Mock Assessment Drive
               AND company_id = 414
          ) fcm
    LEFT JOIN reporting_deliver.dim_companyuser dcu
        ON fcm.interviewer_community_user_id = dcu.id
    JOIN (SELECT *
          FROM reporting_deliver.dim_positions 
          WHERE title IN (
               'Front End Developer',
              'Front-End Developer',
              'Senior Front End Developer - 1+',
              'Senior Front End Developer I',
              'Sr UI/UX Designer',
              'Sr. UX Designer'
            )
          ) dp
        ON fcm.position_id = dp.id
    LEFT JOIN reporting_deliver.dim_companydepartment AS dcd
        ON fcm.position_id = dcd.position_id
    LEFT JOIN (
        SELECT *,
               TRUE     AS new_is_round_passed,
               ROW_NUMBER() OVER (PARTITION BY company_id, recruitment_drive_id, position_id, candidate_application_id ORDER BY process_stage_order) AS ranks
        FROM (
          SELECT company_id,
                 recruitment_drive_id,
                 position_id,
                 candidate_application_id,
                 stage_id,   
                 title,
                 process_stage_order,
                 custom_process_type,
                 previous_stage_id
          FROM (
            SELECT *,
            lag(stage_id) OVER (PARTITION BY company_id, recruitment_drive_id, position_id, candidate_application_id ORDER BY process_stage_order) AS previous_stage_id
            FROM (
                SELECT DISTINCT 
                    fcm.company_id,
                    fcm.candidate_application_id,
                    fcm.recruitment_drive_id,
                    fcm.position_id,
                    fcm.stage_id,
                    rdps.title,
                    rdps.process_stage_order,
                    rdps.custom_process_type
                FROM reporting_deliver.fact_companymetrics fcm
                  LEFT JOIN recruitments_recruitmentdriveprocessstage rdps
                    ON fcm.stage_id = rdps.id
                WHERE rdps.process_stage_order NOT IN (-30, -20, -10, 990, 1000, 1010, 1020, 1025, 1030, 1040, 1050)
                OR (fcm.ds_process_type IN ('applied', 'shortlisted', 'selected', 'offered', 'closures', 'non_closures'))
            ) fcm
          ) fcm
          WHERE title NOT IN ('applied', 'shortlisted', 'application_not_selected', 'absent',
                              'selected', 'not_selected', 'on_hold', 'process_completed',
                              'offered', 'closures', 'non_closures')
                AND custom_process_type = 1
        ) f
      ) ips
        ON ips.company_id = fcm.company_id
          AND ips.recruitment_drive_id = fcm.recruitment_drive_id
          AND ips.candidate_application_id = fcm.candidate_application_id
          AND ips.position_id = fcm.position_id
          AND ips.previous_stage_id = fcm.stage_id
          AND ips.ranks = 1
      JOIN scores swr
        ON swr.candidate_application_id = fcm.candidate_application_id
          AND swr.recruitment_drive_id = fcm.recruitment_drive_id
    GROUP BY fcm.company_id, fcm.candidate_application_id, fcm.recruitment_drive_id, fcm.resume_id,
          fcm."degree", fcm.stream, fcm.offer_job_id, fcm.joining_status, fcm.college_aggregate_score,
          fcm.calyx_score, fcm.school10_score, fcm.school12_score, swr.written_score, swr.mcq_score,
          swr.candidate_id
  ) fcm
  LEFT JOIN candidates_candidateapplication AS ca
    ON fcm.candidate_application_id = ca.id
  LEFT JOIN reporting_deliver.dim_candidate_application AS dca
      ON fcm.candidate_application_id = dca.id
  LEFT JOIN reporting_deliver.dim_resume dr
      ON fcm.resume_id = dr.resume_id
  LEFT JOIN reporting_deliver.dim_job_offer djo
      ON djo.offer_job_id = fcm.offer_job_id
  WHERE fcm.company_id = 414
) chd
```


## jtg_round_wise_customscore_data.sql

```
WITH scores AS (
  SELECT swr.candidate_application_id,
               swr.candidate_id,
               swr.recruitment_drive_id,
               (ARRAY_AGG(swr.mcq_score) FILTER (WHERE swr.mcq_score IS NOT NULL))[1]         AS mcq_score,
               (ARRAY_AGG(swr.written_score) FILTER (WHERE swr.written_score IS NOT NULL))[1] AS written_score
        FROM (
          SELECT candidate_application_id,
                 candidate_id,
                 recruitment_drive_id,
                 stage_id,
                 custom_process_type,
                 score_type,
                 score_label,
                 process_order,
                 CASE
                    WHEN score_label IN ('MCQ',
                                      'FE Freshers- 2021 Batch- November- Day 1 - C/C++ MCQs',
                                      'FE Freshers- Objective- 2021 Batch- November- Day 2 - C/C++ MCQs',
                                      'FE Freshers- Objective- 2021 Batch- November- Day 3 - C/C++ MCQs',
                                      'FE Freshers- Objective- 2021 Batch- November- Day 4 - C/C++ MCQs',
                                      'FE MCQ',
                                      'Total',
                                      'Only Front End - Objective - 1',
                                      'FE+SD Objective 2021 - 1',
                                      'Only Front End - Objective - 2',
                                      'FE Freshers - Interns (MCQs) - 2021 Batch - C/C++ MCQs',
                                      'FE Freshers - Interns (New) - Day 2 - FE MCQs',
                                      'FE Freshers - Interns (New) - Day 3 - FE MCQs',
                                      'FE Freshers - Interns (New) - Day 4 - FE MCQs',
                                      'FE Freshers - Interns (New) - FE MCQs',
                                      'FE Freshers - Full Time (Batch 2) - MCQs',
                                      'FE Freshers - Full Time (New) - MCQs')
                           AND max_score > 10
                      THEN (score/max_score * 100)
                    ELSE NULL
                 END    AS mcq_score,
                 CASE
                    WHEN score_label IN ('Total',
                                      'FE Freshers- Subjective- 2021 Batch- November- Day 1 - Coding Questions',
                                      'FE Freshers- Subjective- 2021 Batch- November- Day 2 - Coding Questions',
                                      'FE Freshers- Subjective- 2021 Batch- November- Day 3 - Coding Questions',
                                      'FE Freshers- Subjective- 2021 Batch- November- Day 4 - Coding Questions',
                                      'FE Total',
                                      'Only Front End Subjective Test',
                                      'Only FE Subjective 2020 -1',
                                      'Only FE Subjective - 2021 - Set 1',
                                      'FE Freshers - Interns (New) - Coding Questions',
                                      'FE Freshers - Interns (New) - Day 2 - Coding Questions',
                                      'FE Freshers - Interns (New) - Day 3 - Coding Questions',
                                      'FE Freshers - Interns (New) - Day 4 - Coding Questions',
                                      'FE Freshers- Subjective- 2021 Batch - Coding Questions',
                                      'FE Freshers - Full Time (Batch 2) - Coding Questions',
                                      'FE Freshers - Full Time (New) - Coding Questions'
                    )
                      AND max_score <= 10
                      THEN (score/max_score * 100)
                    ELSE NULL
                 END    AS written_score
          FROM (
            SELECT rdc.candidate_application_id,
                   rc.candidate_id,
                   rdc.recruitment_drive_id,
                   rr.id                    AS stage_id,
                   rr.custom_process_type,
                   array_agg(rc.custom_scores) 
                          FILTER (WHERE rc.custom_scores IS NOT NULL)
                          OVER (PARTITION BY rdc.candidate_application_id, rdc.recruitment_drive_id)
                              AS custom_scores,
                   NULLIF(rc."value", '')::NUMERIC      AS score,
                   NULLIF(Json(rr.value)->>'type', '')  AS score_type,
                   NULLIF(Json(rr.value)->>'label', '') AS score_label,
                   NULLIF(Json(rr.value)->>'order', '') AS process_order,
                   NULLIF(Json(Json(rr.value)->>'validations')->>'max', '')::numeric AS max_score
            FROM recruitments_recruitmentdrivecandidate rdc
              LEFT JOIN (
                SELECT rc.id, candidate_id, recruitment_drive_process_stage_id, status,
                       (jsonb_each_text(custom_scores)).*,
                       rc.custom_scores,
                       ds.title,
                       ds.process_stage_order
                FROM recruitments_candidaterecruitmentdriveprocessstage rc
                LEFT JOIN reporting_deliver.dim_stages ds
                  ON rc.recruitment_drive_process_stage_id = ds.id
                WHERE rc.id NOT IN (1166646)
              ) rc
                ON rdc.id = rc.candidate_id
              LEFT JOIN (
                SELECT id,
                       recruitment_drive_id,
                       custom_process_type,
                       (jsonb_each_text(custom_fields_template)).*
                FROM recruitments_recruitmentdriveprocessstage rr
              ) rr
              ON rr."key" = rc."key"
                AND rr.recruitment_drive_id = rdc.recruitment_drive_id
                AND Json(rr.value)->>'type' = 'number'
          ) swr
          WHERE swr.custom_scores IS NOT NULL
              AND custom_process_type IN (3, 4)
          ORDER BY candidate_application_id, recruitment_drive_id, stage_id, process_order
        ) swr
        GROUP BY swr.candidate_application_id, swr.recruitment_drive_id, swr.candidate_id
)
SELECT *
FROM (
  SELECT fcm.company_id,
         fcm.candidate_application_id,
         fcm.candidate_id,
         fcm.recruitment_drive_id,
         ca.first_name,
         ca.middle_name,
         ca.last_name,
         ca.phone_number,
         ca.email,
         dca.marital_status,
         dca.known_languages,
         ca.blood_group,
         dca.referred,
         ca.current_experience_years,
         ca.current_experience_months,
         ca.current_ctc,
         ca.total_experience_years,
         ca.total_experience_months,
         dca.is_fresher,
         dca.college_name,
         -- university name logic has changed since DS-946. needs to be updated here
        --  dca.university_name,
         fcm.degree,
         fcm.stream,
         dca.gender,
         dca.dob,
         dca.batch,
         dca.graduation_score,
         fcm.college_aggregate_score,
         dca.twelfth_score,
         fcm.school12_score,
         dca.twelfth_board_name,
         dca.tenth_score,
         fcm.school10_score,
         dca.tenth_board_name,
         fcm.calyx_score           AS candidate_score,
         dr.project_skills,
         dr.cert_skills,
         dr.research_skills,
         dr.intern_skills,
         dr.workex_skills,
         dr.training_skills,
         dr.resume_skills,
         dca.permanent_city,
         dca.permanent_state,
         dca.current_city,
         dca.current_state,
         dca.source_type,
         dca.candidate_source,
         fcm.interviewer_names,
         fcm.interview_time_spent_secs,
         fcm.number_of_interviewers,
         fcm.title,
         fcm.departments,
         djo.ctc,
         fcm.joining_status,
         fcm.mcq_score,
         fcm.written_score,
         fcm.is_absent,
         fcm.recruitment_undergoing,
         fcm.is_round_passed,
         fcm.is_offered,
         CASE
            WHEN fcm.is_offered IS FALSE
                AND fcm.recruitment_undergoing IS TRUE
                THEN NULL
            WHEN fcm.is_offered IS FALSE
                AND fcm.is_absent IS TRUE
                THEN -1
            WHEN fcm.is_offered IS TRUE
                THEN 1
            ELSE 0
         END                    AS new_is_offered
  FROM (
    SELECT
         fcm.company_id,
         fcm.candidate_application_id,
         swr.candidate_id,
         fcm.recruitment_drive_id,
         fcm.resume_id,
         fcm.offer_job_id,
         fcm.degree,
         fcm.stream,
         fcm.college_aggregate_score,
         fcm.calyx_score,
         fcm.school10_score,
         fcm.school12_score,
         SUM(COALESCE(fcm.interview_time_spent_secs, 0))     AS interview_time_spent_secs,
         ARRAY_AGG(DISTINCT dcu.name)                        AS interviewer_names,
         COUNT(DISTINCT dcu.name)                            AS number_of_interviewers,
         ARRAY_AGG(DISTINCT dp.title)                        AS title,
         ARRAY_AGG(DISTINCT dcd.department_name)             AS departments,
         fcm.joining_status,
         bool_or(fcm.recruitment_undergoing)                 AS recruitment_undergoing,
         bool_or(fcm.is_absent)                              AS is_absent,
         bool_or(COALESCE(ips.new_is_round_passed, FALSE))   AS is_round_passed,
         bool_or(fcm.is_offered)                             AS is_offered,
         swr.written_score,
         swr.mcq_score
    FROM (
          SELECT *,
                 CASE
                    WHEN fcm.is_round_passed = 'absent'
                      THEN TRUE
                    ELSE FALSE
                 END                                      AS is_absent,
                 CASE
                    WHEN fcm.ds_process_type IN ('offered')
                        AND fcm.is_round_passed = 'selected'
                    THEN TRUE
                    ELSE FALSE
                 END                                      AS is_offered,
                 CASE
                    WHEN fcm.next_candidate_stage IS NULL
                        AND fcm.ds_process_type NOT IN ('applied', 'shortlisted', 'application_not_selected', 'absent',
                              'selected', 'not_selected', 'on_hold', 'process_completed',
                              'offered', 'closures', 'non_closures')
                        THEN TRUE
                    ELSE FALSE
                 END                                      AS recruitment_undergoing
          FROM reporting_deliver.fact_companymetrics fcm
          WHERE recruitment_drive_id NOT IN (10118) --Mock Assessment Drive
               AND company_id = 414
          ) fcm
    LEFT JOIN reporting_deliver.dim_companyuser dcu
        ON fcm.interviewer_community_user_id = dcu.id
    JOIN (SELECT *
          FROM reporting_deliver.dim_positions 
          WHERE title IN (
               'Front End Developer',
              'Front-End Developer',
              'Senior Front End Developer - 1+',
              'Senior Front End Developer I',
              'Sr UI/UX Designer',
              'Sr. UX Designer'
            )
          ) dp
        ON fcm.position_id = dp.id
    LEFT JOIN reporting_deliver.dim_companydepartment AS dcd
        ON fcm.position_id = dcd.position_id
    LEFT JOIN (
        SELECT *,
               TRUE     AS new_is_round_passed,
               ROW_NUMBER() OVER (PARTITION BY company_id, recruitment_drive_id, position_id, candidate_application_id ORDER BY process_stage_order) AS ranks
        FROM (
          SELECT company_id,
                 recruitment_drive_id,
                 position_id,
                 candidate_application_id,
                 stage_id,   
                 title,
                 process_stage_order,
                 custom_process_type,
                 previous_stage_id
          FROM (
            SELECT *,
            lag(stage_id) OVER (PARTITION BY company_id, recruitment_drive_id, position_id, candidate_application_id ORDER BY process_stage_order) AS previous_stage_id
            FROM (
                SELECT DISTINCT 
                    fcm.company_id,
                    fcm.candidate_application_id,
                    fcm.recruitment_drive_id,
                    fcm.position_id,
                    fcm.stage_id,
                    rdps.title,
                    rdps.process_stage_order,
                    rdps.custom_process_type
                FROM reporting_deliver.fact_companymetrics fcm
                  LEFT JOIN recruitments_recruitmentdriveprocessstage rdps
                    ON fcm.stage_id = rdps.id
                WHERE rdps.process_stage_order NOT IN (-30, -20, -10, 990, 1000, 1010, 1020, 1025, 1030, 1040, 1050)
                OR (fcm.ds_process_type IN ('applied', 'shortlisted', 'selected', 'offered', 'closures', 'non_closures'))
            ) fcm
          ) fcm
          WHERE title NOT IN ('applied', 'shortlisted', 'application_not_selected', 'absent',
                              'selected', 'not_selected', 'on_hold', 'process_completed',
                              'offered', 'closures', 'non_closures')
                AND custom_process_type = 1
        ) f
      ) ips
        ON ips.company_id = fcm.company_id
          AND ips.recruitment_drive_id = fcm.recruitment_drive_id
          AND ips.candidate_application_id = fcm.candidate_application_id
          AND ips.position_id = fcm.position_id
          AND ips.previous_stage_id = fcm.stage_id
          AND ips.ranks = 1
      JOIN scores swr
        ON swr.candidate_application_id = fcm.candidate_application_id
          AND swr.recruitment_drive_id = fcm.recruitment_drive_id
    GROUP BY fcm.company_id, fcm.candidate_application_id, fcm.recruitment_drive_id, fcm.resume_id,
          fcm."degree", fcm.stream, fcm.offer_job_id, fcm.joining_status, fcm.college_aggregate_score,
          fcm.calyx_score, fcm.school10_score, fcm.school12_score, swr.written_score, swr.mcq_score,
          swr.candidate_id
  ) fcm
  LEFT JOIN candidates_candidateapplication AS ca
    ON fcm.candidate_application_id = ca.id
  LEFT JOIN reporting_deliver.dim_candidate_application AS dca
      ON fcm.candidate_application_id = dca.id
  LEFT JOIN reporting_deliver.dim_resume dr
      ON fcm.resume_id = dr.resume_id
  LEFT JOIN reporting_deliver.dim_job_offer djo
      ON djo.offer_job_id = fcm.offer_job_id
  WHERE fcm.company_id = 414
) chd
```

## jtg_round_wise_stagecall_Data.sql


```
SELECT *
FROM (
  SELECT fcm.company_id,
         fcm.candidate_application_id,
         fcm.recruitment_drive_id,
         ca.first_name,
         ca.middle_name,
         ca.last_name,
         ca.phone_number,
         ca.email,
         dca.marital_status,
         dca.known_languages,
         ca.blood_group,
         dca.referred,
         ca.current_experience_years,
         ca.current_experience_months,
         ca.current_ctc,
         ca.total_experience_years,
         ca.total_experience_months,
         dca.is_fresher,
         dca.college_name,
         -- university name logic has changed since DS-946. needs to be updated here
        --  dca.university_name,
         fcm.degree,
         fcm.stream,
         dca.gender,
         dca.dob,
         dca.batch,
         dca.graduation_score,
         fcm.college_aggregate_score,
         dca.twelfth_score,
         fcm.school12_score,
         dca.twelfth_board_name,
         dca.tenth_score,
         fcm.school10_score,
         dca.tenth_board_name,
         fcm.calyx_score           AS candidate_score,
         dr.project_skills,
         dr.cert_skills,
         dr.research_skills,
         dr.intern_skills,
         dr.workex_skills,
         dr.training_skills,
         dr.resume_skills,
         dca.permanent_city,
         dca.permanent_state,
         dca.current_city,
         dca.current_state,
         dca.source_type,
         dca.candidate_source,
         fcm.interviewer_names,
         fcm.interview_time_spent_secs,
         fcm.number_of_interviewers,
         fcm.title,
         fcm.departments,
         djo.ctc,
         fcm.objective_stage_call,
         fcm.written_stage_call,
         fcm.tech1_stage_call,
         fcm.tech2_stage_call,
         fcm.tech3_stage_call,
         fcm.joining_status,
         fcm.is_absent,
         fcm.recruitment_undergoing,
         fcm.is_round_passed,
         fcm.is_offered,
         CASE
            WHEN fcm.is_offered IS FALSE
                AND fcm.recruitment_undergoing IS TRUE
                THEN NULL
            WHEN fcm.is_offered IS FALSE
                AND fcm.is_absent IS TRUE
                THEN -1
            WHEN fcm.is_offered IS TRUE
                THEN 1
            ELSE 0
         END                    AS new_is_offered
  FROM (
    SELECT fcm.company_id,
         fcm.candidate_application_id,
         fcm.recruitment_drive_id,
         fcm.resume_id,
         fcm.offer_job_id,
         fcm.degree,
         fcm.stream,
         fcm.college_aggregate_score,
         fcm.calyx_score,
         fcm.school10_score,
         fcm.school12_score,
         (ARRAY_AGG(objective_stage_call) FILTER (WHERE objective_stage_call IS NOT NULL))[1]   AS objective_stage_call,
         (ARRAY_AGG(written_stage_call) FILTER (WHERE written_stage_call IS NOT NULL))[1]   AS written_stage_call,
         (ARRAY_AGG(tech1_stage_call) FILTER (WHERE tech1_stage_call IS NOT NULL))[1]   AS tech1_stage_call,
         (ARRAY_AGG(tech2_stage_call) FILTER (WHERE tech2_stage_call IS NOT NULL))[1]   AS tech2_stage_call,
         (ARRAY_AGG(tech3_stage_call) FILTER (WHERE tech3_stage_call IS NOT NULL))[1]   AS tech3_stage_call,
         SUM(COALESCE(fcm.interview_time_spent_secs, 0))     AS interview_time_spent_secs,
         ARRAY_AGG(DISTINCT dcu.name)                        AS interviewer_names,
         COUNT(DISTINCT dcu.name)                            AS number_of_interviewers,
         ARRAY_AGG(DISTINCT dp.title)                        AS title,
         ARRAY_AGG(DISTINCT dcd.department_name)             AS departments,
         fcm.joining_status,
         bool_or(fcm.recruitment_undergoing)                 AS recruitment_undergoing,
         bool_or(fcm.is_absent)                              AS is_absent,
         bool_or(COALESCE(ips.new_is_round_passed, FALSE))   AS is_round_passed,
         bool_or(fcm.is_offered)                             AS is_offered
    FROM (
        SELECT *,
               CASE
                  WHEN fcm.round_type = 'objective' AND fcm.latest_round = 1
                     THEN fcm.stage_call
                  ELSE NULL
               END                AS objective_stage_call,
               CASE
                  WHEN fcm.round_type = 'written' AND fcm.latest_round = 1
                     THEN fcm.stage_call
                  ELSE NULL
               END                AS written_stage_call,
               CASE
                  WHEN fcm.round_type = 'tech1' AND fcm.latest_round = 1
                     THEN fcm.stage_call
                  ELSE NULL
               END                AS tech1_stage_call,
               CASE
                  WHEN fcm.round_type = 'tech2' AND fcm.latest_round = 1
                     THEN fcm.stage_call
                  ELSE NULL
               END                AS tech2_stage_call,
               CASE
                  WHEN fcm.round_type = 'tech3' AND fcm.latest_round = 1
                     THEN fcm.stage_call
                  ELSE NULL
               END                AS tech3_stage_call
        FROM (
         SELECT fcm.*,
                ROW_NUMBER() OVER (PARTITION BY fcm.recruitment_drive_id, candidate_application_id, round_type 
                                    ORDER BY ds.process_stage_order DESC) AS latest_round
         FROM (
          SELECT *,
                 CASE
                    WHEN fcm.is_round_passed = 'absent'
                      THEN TRUE
                    ELSE FALSE
                 END                                      AS is_absent,
                 CASE
                    WHEN fcm.ds_process_type IN ('offered')
                        AND fcm.is_round_passed = 'selected'
                    THEN TRUE
                    ELSE FALSE
                 END                                      AS is_offered,
                 CASE
                    WHEN fcm.next_candidate_stage IS NULL
                        AND fcm.ds_process_type NOT IN ('applied', 'shortlisted', 'application_not_selected', 'absent',
                              'selected', 'not_selected', 'on_hold', 'process_completed',
                              'offered', 'closures', 'non_closures')
                        THEN TRUE
                    ELSE FALSE
                 END                                      AS recruitment_undergoing,
                 CASE
                    WHEN fcm.ds_process_type IN ('sd objective assessment-day 5', 'assessment', 'qt assessment',
                        'revised assessment round', 'only fe qt objective 2', 'sd objective assessment-day 2',
                        'assessment round batch 2', 'assessment round', 'front end assessment', 'mock assessment',
                        'objective assessment- biet', 'fe+sd objective 2021 - 1', 'front end assessment- day 4',
                        'qt fe | sd+fe| set 3', 'only fe 2020-21 batch-obj-set1', 'quick test | sd |set 6',
                        'objective assessment- day 2', 'qt only fe - set 1', 'qt only fe new - set 1', 'sd objective qt -2',
                        'sd objective qt set 1', 'qt fe+sd | fe set 4', 'objective assessment- day 4', 'quick test | sd | set 5',
                        'quick test | sd - 4', 'sd objective qt - 6', 'objective assessment- day 1', 'qt - new fe - set 2', 
                        'fe qt | batch2', 'sd objective assessment', 'quick test | north cap drive', 'objective assessment- jc bose',
                        'objective assessment', 'assessment round - 1', 'objective assessment- day 3', 'qt objective - 6', 
                        'front end assessment- day 2', 'new test qt', 'aptitude test', 'client eng aptitude test', 
                        'objective assessment-day 3', 'only fe qt objective 1', 'online test- python developer', 'front end assessment- day 3',
                        'qt online test', 'quick test | sd+fe | set 2', 'sd objective assessment-day 4', 'hr aptitude assessment', 
                        'quick test | sd+fe | set-2', 'qt mcq - set 1', 'sd objective assessment-day 3', 'old assessment round', 'qa assessment round - 1',
                        'qt | only fe - set 2', 'front end assessment- mcqs')
                       THEN 'objective'
                    WHEN fcm.ds_process_type IN ('assignment 2', 'online test', 'subjective', 'written', 'assignment', 'written - online + subjective', 
                        'subjective test', 'written test', 'written - online', 'subjective assessment round','coding test day 1 batch 1','interview round 1- coding-ymca',
                        'interview round 1- coding','subjective assessment','subjectiveassessment-day 3-old','subjective assessment- day 2',
                        'subjective assessment- day 4','subjective assessment- day 1','coding test','sd subjective assessment','fe+sd subjective 2021 - 1',
                        'only fe subjective set-1','coding test day 2 batch 1','interview round 1- coding-kiit','sd subjective assessment-day 4','subjective assessment - day 3',
                        'qt - fe subjective test - set2','sd subjective assessment-day 5','subjective assessment- day 3','front end subjective test','sd qt subjective set 2',
                        'qt - subjective - set 1','only fe subjective 2021 - 1','subjective assessment-jc bose','sd subjective assessment-day 2','subjective assessment - day 1',
                        'interview round 1- coding test','coding test day 1 batch 2','sd subjective qt - 6','coding test day 2 batch 2','sd qt subjective  set 1')
                        THEN 'written'
                    WHEN fcm.ds_process_type IN ('telephonic discussion','interview round 1','fe tech round 1','round 1 interview','technical round 1a','interview round 1 a',
                      'technical discussion 1','technical round 1','interview round 1a','telephonic round','face to face interview','on-site technical round','screening round',
                      'telephonic screening','skype/hangout interview','telephonic interview','php tech round')
                      THEN 'tech1'
                    WHEN fcm.ds_process_type IN ('technical discussion 2','interview round 2','technical round 2','interview round 2a','assessment round-2')
                      THEN 'tech2'
                    WHEN fcm.ds_process_type IN ('technical discussion 3','technical round 3','pre-final round','interview round 5','interview round 4',
                        'interview round 6','interview round 3','interview round 3a','technical round 4')
                      THEN 'tech3'
                    ELSE NULL
                   END              AS round_type
          FROM reporting_deliver.fact_companymetrics fcm
          WHERE company_id = 414
          ) fcm
          LEFT JOIN reporting_deliver.dim_stages AS ds
            ON fcm.stage_id = ds.id
        ) fcm
      )fcm
    LEFT JOIN reporting_deliver.dim_companyuser dcu
        ON fcm.interviewer_community_user_id = dcu.id
    LEFT JOIN reporting_deliver.dim_positions AS dp
        ON fcm.position_id = dp.id
    LEFT JOIN reporting_deliver.dim_companydepartment AS dcd
        ON fcm.position_id = dcd.position_id
    LEFT JOIN (
        SELECT *,
               TRUE     AS new_is_round_passed,
               ROW_NUMBER() OVER (PARTITION BY company_id, recruitment_drive_id, position_id, candidate_application_id ORDER BY process_stage_order) AS ranks
        FROM (
          SELECT company_id,
                 recruitment_drive_id,
                 position_id,
                 candidate_application_id,
                 stage_id,   
                 title,
                 process_stage_order,
                 custom_process_type,
                 previous_stage_id
          FROM (
            SELECT *,
            lag(stage_id) OVER (PARTITION BY company_id, recruitment_drive_id, position_id, candidate_application_id ORDER BY process_stage_order) AS previous_stage_id
            FROM (
                SELECT DISTINCT 
                    fcm.company_id,
                    fcm.candidate_application_id,
                    fcm.recruitment_drive_id,
                    fcm.position_id,
                    fcm.stage_id,
                    rdps.title,
                    rdps.process_stage_order,
                    rdps.custom_process_type
                FROM reporting_deliver.fact_companymetrics fcm
                  LEFT JOIN recruitments_recruitmentdriveprocessstage rdps
                    ON fcm.stage_id = rdps.id
                WHERE rdps.process_stage_order NOT IN (-30, -20, -10, 990, 1000, 1010, 1020, 1025, 1030, 1040, 1050)
                OR (fcm.ds_process_type IN ('applied', 'shortlisted', 'selected', 'offered', 'closures', 'non_closures'))
            ) fcm
          ) fcm
          WHERE title NOT IN ('applied', 'shortlisted', 'application_not_selected', 'absent',
                              'selected', 'not_selected', 'on_hold', 'process_completed',
                              'offered', 'closures', 'non_closures')
                AND custom_process_type = 1
        ) f
      ) ips
        ON ips.company_id = fcm.company_id
          AND ips.recruitment_drive_id = fcm.recruitment_drive_id
          AND ips.candidate_application_id = fcm.candidate_application_id
          AND ips.position_id = fcm.position_id
          AND ips.previous_stage_id = fcm.stage_id
          AND ips.ranks = 1
    GROUP BY fcm.company_id, fcm.candidate_application_id, fcm.recruitment_drive_id, fcm.resume_id,
          fcm."degree", fcm.stream, fcm.offer_job_id, fcm.joining_status, fcm.college_aggregate_score,
          fcm.calyx_score, fcm.school10_score, fcm.school12_score
  ) fcm
  LEFT JOIN candidates_candidateapplication AS ca
    ON fcm.candidate_application_id = ca.id
  LEFT JOIN reporting_deliver.dim_candidate_application AS dca
      ON fcm.candidate_application_id = dca.id
  LEFT JOIN reporting_deliver.dim_resume dr
      ON fcm.resume_id = dr.resume_id
  LEFT JOIN reporting_deliver.dim_job_offer djo
      ON djo.offer_job_id = fcm.offer_job_id
) chd
```

## dim_offerjob.sql

```
SELECT
  sj.offer_job_id,
  sj.base_job_id,
  sj.placement_drive_id,
  sj.company_id,
  sj.ctc,
  sj.gross_pay,
  sj.title,
  sj.expected_joining,
  sj.created_at,
  sj.pan_india,
  sj.student_id,
  sj.graduation_year,
  sj.job_offer_type,
  sj.pool_offer_job_id,
  cd.college_id,
  jic.id                           AS category_id,
  jic.name                         AS category_name,
  jic.min_package,
  jic.max_package,
  coalesce(pj.ctc, pj.ctc_max, -1) AS max_ctc,
  jic.max_allowed_offers_in_this_category,
  NULL :: INT                      AS job_offer_rank,
  sj.student_offer_updated_at,
  sj.offer_status,
  sj.published_at,
  sj.is_offer_letter_uploaded,
  GREATEST(
    sj.updated_at, ojb.updated_at,
    jic.updated_at, jit.updated_at, 
    jitc.updated_at
  )                                AS last_updated_at,
  sj.active
FROM college_reporting_support.support_job sj
LEFT JOIN colleges_collegecourse cc
  ON sj.current_course_id = cc.id
LEFT JOIN colleges_collegedepartment cd
  ON cc.college_department_id = cd.id
LEFT JOIN (
  SELECT
    offer_job_id,
    max(updated_at)                                            updated_at
  FROM placements_offerjobbonus
  WHERE active
  GROUP BY offer_job_id
  ) ojb
  ON sj.offer_job_id = ojb.offer_job_id
LEFT JOIN placements_jobinternshipcategory jic
  ON sj.category_id = jic.id
LEFT JOIN placements_jobinternshiptype jit
  ON sj.job_type_id = jit.id
LEFT JOIN placements_jobinternshiptypecategory jitc
  ON jit.category_id = jitc.id
LEFT JOIN placements_job pj
  ON pj.basejob_ptr_id = sj.base_job_id 
  ```
  
  ##002_support_job.sql
  
  ```
  SELECT
  oj.id                            AS offer_job_id,
  COALESCE(oj.job_id, poj.job_id)  AS base_job_id,
  oj.placement_drive_id,
  oj.company_id,
  oj.category_id,
  oj.job_type_id,
  ojs.id 						               AS offer_job_student_id,
  COALESCE(ojs.active, TRUE) AND COALESCE(oj.active, TRUE) AND COALESCE(p.active, TRUE) AS active,
  oj.ctc,
  oj.gross_pay,
  COALESCE(oj.title, 'Not Available') AS title,
  oj.expected_joining,
  oj.created_at                    AS created_at,
  oj.pool_offer_job_id,
  COALESCE(oj.pan_india, FALSE)    AS pan_india,
  oj.offer_type                    AS job_offer_type,
  ojs.student_id                   AS student_id,
  s.graduation_year,
  s.current_course_id,
  ojs.created_at                   AS student_offer_created_at,
  ojs.updated_at                   AS student_offer_updated_at,
  s.updated_at                     AS student_updated_at,
  oj.offer_status,
  ojs.published_at,
  COALESCE(offer_letter.active, FALSE) 
    AND COALESCE(offer_letter.is_saved, FALSE) AS "is_offer_letter_uploaded",
  GREATEST(
    oj.updated_at, p.updated_at, 
    ojs.updated_at, s.updated_at,
    offer_letter.updated_at
  )                                AS updated_at
FROM placements_offerjob oj
LEFT JOIN placements_offerjobstudent ojs
  ON oj.id = ojs.offer_job_id
LEFT JOIN students_student s
  ON ojs.student_id = s.id
LEFT JOIN placements_placementdrive p
  ON oj.placement_drive_id = p.id
LEFT JOIN placements_offerjob poj
  ON oj.pool_offer_job_id = poj.id
LEFT JOIN (
  SELECT 
    * 
  FROM 
    (
      SELECT 
        *, 
        ROW_NUMBER() OVER (
          PARTITION BY object_id 
          ORDER BY updated_at DESC
        ) AS row_num 
      FROM 
        attachments_attachment
      WHERE 
        content_type_id = (SELECT id FROM django_content_type WHERE app_label = 'placements' AND model = 'offerjob')
        AND category = 7 --offer letter
    ) AS attachments
  WHERE 
    attachments.row_num = 1 -- this is done to ensure 1 record is fetched for each offer
) offer_letter
  ON offer_letter.object_id = oj.id
WHERE (oj.approval_status = 1 OR oj.created_via = 2) -- considering only approved offers or offers created via others
AND oj.is_partial_offer != TRUE -- removing partial offers
AND NOT EXISTS ( -- removing pool campus duplicate offers
  SELECT 1 FROM placements_offerjob poj2 WHERE poj2.pool_offer_job_id = oj.id
)
```

## job_offer_rank.sql

```
-- add offer rank to job offers
UPDATE reporting_deliver.dim_job_offer AS off1
SET job_offer_rank = off2.row_num
FROM (
	SELECT
        offer_job_id,
		row_number() OVER (PARTITION BY student_id ORDER BY ctc DESC NULLS LAST) AS row_num
		FROM reporting_deliver.dim_job_offer
		WHERE active
)off2 WHERE off1.offer_job_id = off2.offer_job_id;
```

## dim_candidate_job_offer.sql

``` BEGIN TRANSACTION;

UPDATE reporting_deliver.dim_candidate_job_offer AS off1
SET candidate_job_offer_rank = off2.row_num
FROM (
	SELECT
        offer_job_id,
		row_number() OVER (PARTITION BY candidate_application_id ORDER BY ctc DESC NULLS LAST) AS row_num
		FROM reporting_deliver.dim_candidate_job_offer
)off2 WHERE off1.offer_job_id = off2.offer_job_id;

END TRANSACTION;
```

## dim_candidate_job_offer.sql

```
SELECT
  oj.id                            AS offer_job_id,
  oj.job_id                        AS base_job_id,
  oj.recruitment_drive_id,
  oj.company_id,
  pbj.title                        AS offer_position_title,
  oj.ctc,
  oj.gross_pay,
  oj.other_benefits,
  oj.title,
  oj.bond,
  oj.other_details,
  oj.description,
  oj.remuneration_other_details,
  oj.expected_joining,
  oj.created_at                    AS created_at,
  coalesce(oj.pan_india, FALSE)    AS pan_india,
  ojb.bonuses,
  ca.id                            AS candidate_application_id,
  ca.graduation_year,
  cd.college_id,
  jic.id                           AS category_id,
  jic.name                         AS category_name,
  jic.min_package,
  jic.max_package,
  jic.max_allowed_offers_in_this_category,
  jit.name                         AS job_type_name,
  coalesce(jit.is_approved, FALSE) AS job_type_is_approved,
  jitc.name                        AS job_type_category_name,
  NULL :: INT                      AS candidate_job_offer_rank,
  ojc.updated_at                   AS candidate_offer_updated_at,
  greatest(oj.updated_at, rd.updated_at, ojb.updated_at,
           jic.updated_at, jit.updated_at, jitc.updated_at
    )                              AS last_updated_at
FROM recruitments_offers_offerjobcandidate ojc
LEFT JOIN placements_offerjob oj
  ON ojc.offer_job_id = oj.id
LEFT JOIN placements_basejob pbj
  ON oj.job_id = pbj.id
LEFT JOIN recruitments_recruitmentdrivecandidate rdc
  ON ojc.recruitment_drive_candidate_id = rdc.id
LEFT JOIN candidates_candidateapplication ca
  ON rdc.candidate_application_id = ca.id
LEFT JOIN colleges_collegecourse cc
  ON ca.course_id = cc.id
LEFT JOIN colleges_collegedepartment cd
  ON cc.college_department_id = cd.id
LEFT JOIN recruitments_recruitmentdrive rd
  ON oj.recruitment_drive_id = rd.id
LEFT JOIN (
  SELECT
    offer_job_id,
    json_object(array_agg(name), array_agg(amount :: VARCHAR)) bonuses,
    max(updated_at)                                            updated_at
  FROM placements_offerjobbonus
  GROUP BY offer_job_id
  ) ojb
  ON oj.id = ojb.offer_job_id
LEFT JOIN placements_jobinternshipcategory jic
  ON oj.category_id = jic.id
LEFT JOIN placements_jobinternshiptype jit
  ON oj.job_type_id = jit.id
LEFT JOIN placements_jobinternshiptypecategory jitc
  ON jit.category_id = jitc.id
```

## offfered_data_calyx.sql

```
--updated skills_updated_at logic
SELECT
  student.student_id          AS id,
  student.resume_id           AS resume_id,
  student.college_id,
  college.name                AS college_name,
  student.graduation_year,
  cw.weighted_score           AS college_weight,
  ss.school10_score           AS school10_score,
  ss.school12_score           AS school12_score,
  ss.college_aggregate_score  AS college_aggregate_score,
  student.degree_name         AS degree,
  student.specialization_name AS stream,
  student.title               AS job_title,
  student.ctc                 AS ctc,
  resume.project_skills,
  resume.cert_skills,
  resume.research_skills,
  resume.intern_skills,
  resume.workex_skills,
  resume.training_skills,
  resume.resume_skills,
  resume.skills_last_updated_at,
  'Student'                   AS object_type
FROM
  (
  SELECT *
  FROM (
    SELECT
      fact.student_id,
      fact.resume_id,
      st.graduation_year,
      st.current_course_id,
      fact.college_id,
      oj.student_offer_updated_at,
      oj.title,
      oj.ctc,
      oj.company_id,
      specialization_name,
      degree_name,
      RANK() OVER (PARTITION BY st.resume_id, oj.company_id ORDER BY oj.student_offer_updated_at DESC) AS row_num
    FROM (
      SELECT
        student_id,
        offer_job_id,
        target_company_id,
        specialization_name,
        degree_name,
        college_id,
        resume_id
      FROM reporting_deliver.fact_college_metrics
      WHERE student_id IS NOT NULL
        AND offer_job_id IS NOT NULL
        AND degree_name <> ''
        AND specialization_name <> ''
        AND resume_id IS NOT NULL
    ) AS fact
    INNER JOIN
    (
      SELECT *
      FROM (
        SELECT
          student_id,
          graduation_year,
          resume_id,
          current_course_id,
          college_id,
          last_updated_at,
          RANK() OVER (PARTITION BY enrollment_number, current_course_id ORDER BY student_updated_at DESC) AS row_num
        FROM reporting_deliver.dim_student
        WHERE graduation_year IS NOT NULL
           ) stu
      WHERE row_num = 1
    ) st
      ON fact.student_id = st.student_id
    LEFT JOIN reporting_deliver.dim_job_offer oj
      ON fact.offer_job_id = oj.offer_job_id
  ) AS s
  WHERE row_num = 1
    AND ctc > 0
    AND title NOT IN ('Not Mentioned', 'Multiple Positions', 'Not Specified')
) AS student
LEFT JOIN reporting_deliver.dim_colleges college
  ON student.college_id = college.college_id
LEFT JOIN reporting_deliver.college_weights cw
  ON cw.college_name = college.name
LEFT JOIN reporting_clean.student_scores ss
  ON ss.student_id = student.student_id
LEFT JOIN reporting_deliver.dim_resume resume
  ON student.resume_id = resume.resume_id
WHERE (ss.school12_score BETWEEN 0 AND 100 OR ss.school12_score IS NULL)
  AND (ss.school10_score BETWEEN 0 AND 100 OR ss.school10_score IS NULL)
  AND college.is_demo IS FALSE
UNION
SELECT
  fact.id,
  fact.resume_id,
  fact.college_id,
  fact.college_name,
  fact.graduation_year,
  COALESCE(cw.weighted_score, 37.12)::NUMERIC         AS college_weight,
  COALESCE(fact.school10_score, 40)::NUMERIC          AS school10_score,
  COALESCE(fact.school12_score, 40)::NUMERIC          AS school12_score,
  COALESCE(fact.college_aggregate_score, 30)::NUMERIC AS college_aggregate_score,
  fact.degree,
  fact.stream,
  job.title                                           AS job_title,
  COALESCE(job.ctc, 0)::NUMERIC                       AS ctc,
  resume.project_skills,
  resume.cert_skills,
  resume.research_skills,
  resume.intern_skills,
  resume.workex_skills,
  resume.training_skills,
  resume.resume_skills,
  resume.skills_last_updated_at,
  'Candidate'                                         AS object_type
FROM (
  SELECT DISTINCT
    community_candidate_id AS id,
    resume_id,
    college_id,
    college_name,
    student_id,
    graduation_year,
    school10_score,
    school12_score,
    college_aggregate_score,
    degree,
    stream,
    offer_job_id,
    event_type
  FROM reporting_deliver.fact_companymetrics
  WHERE (school12_score BETWEEN 0 AND 100 OR school12_score IS NULL)
    AND (school10_score BETWEEN 0 AND 100 OR school10_score IS NULL)
    AND event_type = 'campus'
    AND degree <> ''
    AND stream <> ''
    AND student_id IS NULL
    AND resume_id IS NOT NULL
    AND graduation_year IS NOT NULL
    AND college_name IS NOT NULL
) fact
LEFT JOIN (
  SELECT *
  FROM reporting_clean.college_weights
  WHERE generation_timestamp = (
    SELECT
      MAX(generation_timestamp)
    FROM reporting_clean.college_weights
    )
) AS cw
  ON cw.college_name = fact.college_name
LEFT JOIN reporting_deliver.dim_job_offer job
  ON fact.offer_job_id = job.offer_job_id
LEFT JOIN reporting_deliver.dim_resume resume
  ON fact.resume_id = resume.resume_id
WHERE job.ctc > 0
```

## candidate_filtering_sql.sql


```
--updated skills_updated_at logic
SELECT
  student.student_id          AS id,
  student.resume_id           AS resume_id,
  student.college_id,
  college.name                AS college_name,
  student.graduation_year,
  cw.weighted_score           AS college_weight,
  ss.school10_score           AS school10_score,
  ss.school12_score           AS school12_score,
  ss.college_aggregate_score  AS college_aggregate_score,
  student.degree_name         AS degree,
  student.specialization_name AS stream,
  student.title               AS job_title,
  student.ctc                 AS ctc,
  resume.project_skills,
  resume.cert_skills,
  resume.research_skills,
  resume.intern_skills,
  resume.workex_skills,
  resume.training_skills,
  resume.resume_skills,
  resume.skills_last_updated_at,
  'Student'                   AS object_type
FROM
  (
  SELECT *
  FROM (
    SELECT
      fact.student_id,
      fact.resume_id,
      st.graduation_year,
      st.current_course_id,
      fact.college_id,
      oj.student_offer_updated_at,
      oj.title,
      oj.ctc,
      oj.company_id,
      specialization_name,
      degree_name,
      RANK() OVER (PARTITION BY st.resume_id, oj.company_id ORDER BY oj.student_offer_updated_at DESC) AS row_num
    FROM (
      SELECT
        student_id,
        offer_job_id,
        target_company_id,
        specialization_name,
        degree_name,
        college_id,
        resume_id
      FROM reporting_deliver.fact_college_metrics
      WHERE student_id IS NOT NULL
        AND offer_job_id IS NOT NULL
        AND degree_name <> ''
        AND specialization_name <> ''
        AND resume_id IS NOT NULL
    ) AS fact
    INNER JOIN
    (
      SELECT *
      FROM (
        SELECT
          student_id,
          graduation_year,
          resume_id,
          current_course_id,
          college_id,
          last_updated_at,
          RANK() OVER (PARTITION BY enrollment_number, current_course_id ORDER BY student_updated_at DESC) AS row_num
        FROM reporting_deliver.dim_student
        WHERE graduation_year IS NOT NULL
           ) stu
      WHERE row_num = 1
    ) st
      ON fact.student_id = st.student_id
    LEFT JOIN reporting_deliver.dim_job_offer oj
      ON fact.offer_job_id = oj.offer_job_id
  ) AS s
  WHERE row_num = 1
    AND ctc > 0
    AND title NOT IN ('Not Mentioned', 'Multiple Positions', 'Not Specified')
) AS student
LEFT JOIN reporting_deliver.dim_colleges college
  ON student.college_id = college.college_id
LEFT JOIN reporting_deliver.college_weights cw
  ON cw.college_name = college.name
LEFT JOIN reporting_clean.student_scores ss
  ON ss.student_id = student.student_id
LEFT JOIN reporting_deliver.dim_resume resume
  ON student.resume_id = resume.resume_id
WHERE (ss.school12_score BETWEEN 0 AND 100 OR ss.school12_score IS NULL)
  AND (ss.school10_score BETWEEN 0 AND 100 OR ss.school10_score IS NULL)
  AND college.is_demo IS FALSE
UNION
SELECT
  fact.id,
  fact.resume_id,
  fact.college_id,
  fact.college_name,
  fact.graduation_year,
  COALESCE(cw.weighted_score, 37.12)::NUMERIC         AS college_weight,
  COALESCE(fact.school10_score, 40)::NUMERIC          AS school10_score,
  COALESCE(fact.school12_score, 40)::NUMERIC          AS school12_score,
  COALESCE(fact.college_aggregate_score, 30)::NUMERIC AS college_aggregate_score,
  fact.degree,
  fact.stream,
  job.title                                           AS job_title,
  COALESCE(job.ctc, 0)::NUMERIC                       AS ctc,
  resume.project_skills,
  resume.cert_skills,
  resume.research_skills,
  resume.intern_skills,
  resume.workex_skills,
  resume.training_skills,
  resume.resume_skills,
  resume.skills_last_updated_at,
  'Candidate'                                         AS object_type
FROM (
  SELECT DISTINCT
    community_candidate_id AS id,
    resume_id,
    college_id,
    college_name,
    student_id,
    graduation_year,
    school10_score,
    school12_score,
    college_aggregate_score,
    degree,
    stream,
    offer_job_id,
    event_type
  FROM reporting_deliver.fact_companymetrics
  WHERE (school12_score BETWEEN 0 AND 100 OR school12_score IS NULL)
    AND (school10_score BETWEEN 0 AND 100 OR school10_score IS NULL)
    AND event_type = 'campus'
    AND degree <> ''
    AND stream <> ''
    AND student_id IS NULL
    AND resume_id IS NOT NULL
    AND graduation_year IS NOT NULL
    AND college_name IS NOT NULL
) fact
LEFT JOIN (
  SELECT *
  FROM reporting_clean.college_weights
  WHERE generation_timestamp = (
    SELECT
      MAX(generation_timestamp)
    FROM reporting_clean.college_weights
    )
) AS cw
  ON cw.college_name = fact.college_name
LEFT JOIN reporting_deliver.dim_job_offer job
  ON fact.offer_job_id = job.offer_job_id
LEFT JOIN reporting_deliver.dim_resume resume
  ON fact.resume_id = resume.resume_id
WHERE job.ctc > 0
```
