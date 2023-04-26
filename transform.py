import pandas as pd
import numpy as np

def applicants(applicants):
    applicants = applicants.dropna(how='all')
    applicants['phone_number'] = applicants['phone_number'].str.replace(
        '(', '').str.replace(')', '').str.replace(' ', '-') 
    # Enforce datetime type
    applicants['dob'] = pd.to_datetime(applicants['dob'], errors='raise', dayfirst=True, format='mixed')
    # Replace invited_date column with the full date
    # First add day to the date
    applicants['invited_date'] = applicants['invited_date'].astype(
        'Int64', errors='raise').apply(str) + ' ' + applicants['month']
    # Convert the mixed date formats to datetime
    applicants['invited_date'] = pd.to_datetime(applicants['invited_date'], errors='raise', dayfirst=True, format='mixed')
    applicants = applicants.drop(columns=['month','id'])

    
    applicants['invited_by'] = applicants['invited_by'].replace({'Bruno Belbrook': 'Bruno Bellbrook', 'Fifi Etton': 'Fifi Eton'})
    applicants = applicants.rename(columns = {'invited_by':'recruiter_name'})
    applicants = applicants.drop_duplicates()
    recruiters = applicants[['recruiter_name']]
    recruiters = recruiters.drop_duplicates()
    locations = applicants[['address', 'postcode', 'city']]
    locations = locations.drop_duplicates()

    return recruiters, locations, applicants


def assessments_txt(assessments_df):
    assessments_df.columns = ["FirstName","LastName","psychometric_result","presentation_result","date","location_name"]

    # Additional cleaning, strip whitespace, enforce data types
    assessments_df.FirstName = assessments_df.FirstName.str.strip(" ")
    assessments_df.LastName = assessments_df.LastName.str.strip(" ")
    assessments_df.psychometric_result = assessments_df.psychometric_result.astype(int)
    assessments_df.presentation_result = assessments_df.presentation_result.astype(int)
    assessments_df.date = assessments_df.date = pd.to_datetime(assessments_df.date)
    assessments_df['name'] = assessments_df['FirstName'].str.cat(assessments_df['LastName'], sep=' ')
    assessments_df.drop(columns = ['FirstName', 'LastName'], inplace =True)
    academy = assessments_df[['location_name']]
    academy = academy.drop_duplicates()

    return academy, assessments_df

def candidates_json(interview_df):
    interview_df['date'] = interview_df['date'].str.replace('//', '/')
    interview_df['date'] = pd.to_datetime(interview_df['date'], errors='raise', dayfirst=True, format='mixed')

    # should normalise names
    # interview_df[['first_name', 'surname']] = interview_df['name'].str.extract(r'^(\S+)\s(.+)$')

    df_strengths = interview_df[['strengths']].explode('strengths')
    df_strengths.rename(columns = {'strengths': 'strength'}, inplace = True)
    df_strengths = df_strengths.dropna()

    df_weaknesses = interview_df[['weaknesses']].explode('weaknesses')
    df_weaknesses.rename(columns = {'weaknesses': 'weakness'}, inplace = True)
    df_weaknesses = df_weaknesses.dropna()

    filtered_df = interview_df.filter(regex='^tech_self_score')
    filtered_df.columns = filtered_df.columns.str.replace("tech_self_score.", "")
    technologies_df = filtered_df.melt(value_vars=filtered_df.columns[:-1], var_name='technology', value_name='score')

    interview_df['result'] = interview_df['result'].replace({'Pass': True, 'Fail': False})
    interview_df.replace({'Yes': True, "No": False}, inplace = True)
    interview_df = interview_df[['name', 'date', 'self_development', 'geo_flex', 'financial_support_self', 'result', 'course_interest']]

    return interview_df, df_strengths, df_weaknesses, technologies_df