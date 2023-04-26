import extract
import transform
import load

talent_extractor = extract.Talent_Extractor()
loader = load.Loader()
# iterate over the generator to process each dataframe one at a time
for df in talent_extractor.applicants_to_df():
    # perform transformation on df
    recruiters,locations,applicants = transform.applicants(df)
    loader.load_recruiters(recruiters)
    loader.load_locations(locations)
    loader.load_applicants(applicants)


for df in talent_extractor.txt_to_df():
    academy, assessments_df  = transform.assessments_txt(df)
    loader.academy_locations(academy)
    loader.assessments(assessments_df)

for df in talent_extractor.jsons_to_df():
    interview_df, df_strengths, df_weaknesses, technologies_df = transform.candidates_json(df)
    applicant_id_df = loader.get_applicant_id_json(interview_df)
    loader.interviews(applicant_id_df, interview_df)
    loader.strengths(df_strengths)
    loader.strengths_junction(applicant_id_df, df_strengths)
    loader.weaknesses(df_weaknesses)
    loader.weaknesses_junction(applicant_id_df, df_weaknesses)
    loader.technologies(technologies_df)


