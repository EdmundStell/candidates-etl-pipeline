import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import numpy as np
import os
from datetime import datetime

class Loader():
    def __init__(self):
        load_dotenv()  # Load environment variables from .env file
        db_password = os.getenv("DB_PASSWORD")
        self.engine = create_engine(f"mssql+pyodbc://sa:{db_password}@localhost:1433/Talent?driver=ODBC+Driver+17+for+SQL+Server")

    def load_recruiters(self, recruiters):
        conn = self.engine.connect()
        query = text('SELECT DISTINCT recruiter_name FROM recruiters;')
        r = conn.execute(query)
        recruiters_db = pd.DataFrame(r)
        recruiters = recruiters.dropna()
        if len(recruiters_db):
            new_recruiters = recruiters.loc[~recruiters['recruiter_name'].isin(recruiters_db['recruiter_name']), :]
        else: # database is empty
            null_row = pd.DataFrame({'recruiter_name': [None]})
            new_recruiters = pd.concat([null_row,recruiters])
        try:
            new_recruiters.to_sql(name='Recruiters', con=self.engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Error: {str(e)}")

        conn.close()

    def load_locations(self, locations):
        conn = self.engine.connect()
        query = text('SELECT DISTINCT * FROM Locations;')
        r = conn.execute(query)
        locations_db = pd.DataFrame(r)
        locations = locations.dropna(how='all')
        if len(locations_db):
            new_locations = locations[~locations.apply(lambda row: (row['address'], row['postcode'], row['city']) in zip(locations_db['address'], locations_db['postcode'], locations_db['city']), axis=1)]
        else: # database is empty
            new_locations = locations
            null_row = pd.DataFrame({'address': [None], 'postcode': [None], 'city': [None]})
            null_row.to_sql(name='Locations', con=self.engine, if_exists='append', index=False)
        try:
            new_locations.to_sql(name='Locations', con=self.engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Error: {str(e)}")
        conn.close()

    def load_applicants(self, applicants):
        # query locations
        conn = self.engine.connect()
        query = text('SELECT * FROM Locations') 
        r = conn.execute(query)
        locations_db = pd.DataFrame(r) 
        applicants.replace(np.nan, 'nan', regex=True, inplace=True)
        locations_db.replace(np.nan, 'nan', regex=True, inplace=True)
        merged_l = pd.merge(applicants, locations_db, on=['address', 'postcode', 'city'], how='left')
        query = text('SELECT * FROM Recruiters') 
        r = conn.execute(query)
        recruiters_db = pd.DataFrame(r) 
        recruiters_db.replace(np.nan, 'nan', regex=True, inplace=True)

        merged_lr = pd.merge(merged_l, recruiters_db, on=['recruiter_name'], how='left')
        merged_lr.drop(columns=['address', 'postcode', 'city', 'recruiter_name'], axis=1, inplace=True)

        query = text('SELECT * FROM Applicants')
        r = conn.execute(query)
        applicants_db = pd.DataFrame(r)

        if len(applicants_db):
            applicants_db[['dob', 'invited_date']] = applicants_db[['dob', 'invited_date']].apply(pd.to_datetime, errors='coerce')
            merged_lr[['dob', 'invited_date']] = merged_lr[['dob', 'invited_date']].astype(str)
            applicants_db[['dob', 'invited_date']] = applicants_db[['dob', 'invited_date']].astype(str)
            applicants_db.drop(columns = ['applicant_id'],inplace=True)
            new_applicants = merged_lr[~merged_lr.apply(lambda row: all(row[col] in applicants_db[col].values for col in applicants_db.columns), axis=1)]
            if len(new_applicants):
                new_applicants[['dob', 'invited_date']] = new_applicants[['dob', 'invited_date']].apply(pd.to_datetime, errors ='coerce')
        else:
            new_applicants = merged_lr
        try:
            new_applicants.to_sql(name='Applicants', con=self.engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Error: {str(e)}")

        conn.close()

    def academy_locations(self, academy_location):
        conn = self.engine.connect()
        query = text('SELECT * FROM Academy_Locations') 
        r = conn.execute(query)
        academy_locations_db = pd.DataFrame(r)

        if len(academy_locations_db):
            new_academy = academy_location.loc[~academy_location['location_name'].isin(academy_locations_db['location_name']), :]
        else: # database is empty
            null_row = pd.DataFrame({'location_name': [None]})
            new_academy = pd.concat([null_row,academy_location])
        try:
            new_academy.to_sql(name='Academy_Locations', con=self.engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Error: {str(e)}")

        conn.close()

    def assessments(self, sparta_day_df):
        conn = self.engine.connect()
        query = text('SELECT * FROM Academy_Locations') 
        r = conn.execute(query)
        academy_locations_db = pd.DataFrame(r)
        merged = pd.merge(sparta_day_df, academy_locations_db, on=['location_name'], how='left')

        date = merged['date'].iloc[0]
        date_str = date.strftime('%Y-%m-%d')

        query = text(f"SELECT name, applicant_id, invited_date FROM Applicants WHERE invited_date <= '{date_str}' ORDER BY invited_date DESC;") 
        r = conn.execute(query)
        applicants_db = pd.DataFrame(r)
        applicants_db['name'] = applicants_db['name'].str.lower()
        merged['name']= merged['name'].str.lower()

        # not guaranteed to catch all duplicate names
        merged = pd.merge(merged, applicants_db, on=['name'], how='left')
        merged.drop_duplicates(subset=['name'], keep='first', inplace=True)

        merged.drop(columns=['location_name', 'name', 'invited_date'], inplace=True)

        query = text(f'SELECT * FROM assessments')
        r = conn.execute(query)
        assessments_db = pd.DataFrame(r)

        if len(assessments_db):
            assessments_db['date'] = assessments_db['date'].astype(str)
            merged['date'] = merged['date'].astype(str)
            new_talent = merged[~merged.apply(lambda row: all(row[col] in assessments_db[col].values for col in assessments_db.columns), axis=1)]
            if not new_talent.empty:
                new_talent['date'] = pd.to_datetime(new_talent['date'])

        else:
            new_talent = merged
        try:
            new_talent.to_sql(name='Assessments', con=self.engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Error: {str(e)}")
        
        conn.close()

    def get_applicant_id_json(self, interview_df):
        conn = self.engine.connect()
        name = interview_df['name'].values[0]
        date = interview_df['date'].values[0]
        query = text("SELECT assessments.applicant_id FROM Assessments JOIN Applicants ON Assessments.applicant_id = Applicants.applicant_id WHERE name LIKE '{}' AND date = '{}'".format(name.replace("'", "''"), date))
        r = conn.execute(query)

        applicant_id_df = pd.DataFrame(r)
        if not len(applicant_id_df):
            incremented_date = pd.Timestamp(date) + pd.DateOffset(months=-1)
            incremented_date = np.datetime64(incremented_date, 'D')
            query = text("SELECT assessments.applicant_id FROM Assessments JOIN Applicants ON Assessments.applicant_id = Applicants.applicant_id WHERE name LIKE '{}' AND date = '{}'".format(name.replace("'", "''"), incremented_date))
            r = conn.execute(query)
            applicant_id_df = pd.DataFrame(r)
        print(applicant_id_df)
        conn.close()
        return applicant_id_df
    
    def interviews(self, applicant_id_df, interview_df):
        interview_df = interview_df.drop(columns=['name', 'date'])
        interview_df = pd.concat([applicant_id_df, interview_df], axis=1)
        conn = self.engine.connect()
        query = text(f'SELECT * FROM interviews')
        r = conn.execute(query)
        interview_db = pd.DataFrame(r)   
        if len(interview_db):
            new_interview = interview_df[~interview_df.apply(lambda row: all(row[col] in interview_db[col].values for col in interview_db.columns), axis=1)]
        else:
            new_interview = interview_df
        try:
            new_interview.to_sql(name = 'interviews', con = self.engine, if_exists = 'append', index = False)
        except Exception as e:
            print(f"Error: {str(e)}")

        conn.close()

    def strengths(self, strengths_df):
        conn = self.engine.connect()
        query = text('SELECT * FROM strengths;')
        r = conn.execute(query)
        strengths_db = pd.DataFrame(r)  
        if len(strengths_db):
            strengths_db = strengths_db.drop(columns = {'strength_id'})
            new_strengths = strengths_df.loc[~strengths_df['strength'].isin(strengths_db['strength']), :]
        else:
            new_strengths = strengths_df
        try:
            new_strengths.to_sql(name = 'strengths', con = self.engine, if_exists = 'append', index = False)
        except Exception as e:
            print(f"Error: {str(e)}")
        
        conn.close()
    
    def strengths_junction(self, applicant_id_df, strengths_df):
        conn = self.engine.connect()
        query = text('SELECT * FROM strengths;')
        r = conn.execute(query)
        strengths_db = pd.DataFrame(r)
        strength_junction_df = strengths_df.merge(strengths_db, on='strength', how='inner')
        applicant_id = applicant_id_df['applicant_id'].values[0]
        strength_junction_df['applicant_id'] = pd.Series(applicant_id, index=strength_junction_df.index)
        strength_junction_df.drop(columns = {'strength'}, inplace = True)
        query = text('SELECT * FROM strengths_junction;')
        r = conn.execute(query)
        strengths_junction_db = pd.DataFrame(r)

        if len(strengths_junction_db):
            new_strengths_junction = strength_junction_df[~strength_junction_df.apply(lambda row: all(row[col] in strengths_junction_db[col].values for col in strengths_junction_db.columns), axis=1)]
        else:
            new_strengths_junction = strength_junction_df
        try:
            new_strengths_junction.to_sql(name = 'strengths_junction', con = self.engine, if_exists = 'append', index = False)
        except Exception as e:
            print(f"Error: {str(e)}")
        
        conn.close()

    def weaknesses(self, weaknesses_df):
        conn = self.engine.connect()
        query = text('SELECT * FROM weaknesses;')
        r = conn.execute(query)
        weaknesses_db = pd.DataFrame(r)  
        if len(weaknesses_db):
            weaknesses_db = weaknesses_db.drop(columns = {'weakness_id'})
            new_weaknesses = weaknesses_df.loc[~weaknesses_df['weakness'].isin(weaknesses_db['weakness']), :]
        else:
            new_weaknesses = weaknesses_df
        try:
            new_weaknesses.to_sql(name = 'weaknesses', con = self.engine, if_exists = 'append', index = False)
        except Exception as e:
            print(f"Error: {str(e)}")
        
        conn.close()

    def weaknesses_junction(self, applicant_id_df, weaknesses_df):
        conn = self.engine.connect()
        query = text('SELECT * FROM weaknesses;')
        r = conn.execute(query)
        weaknesses_db = pd.DataFrame(r)
        weakness_junction_df = weaknesses_df.merge(weaknesses_db, on='weakness', how='inner')
        applicant_id = applicant_id_df['applicant_id'].values[0]
        weakness_junction_df['applicant_id'] = pd.Series(applicant_id, index=weakness_junction_df.index)
        weakness_junction_df.drop(columns = {'weakness'}, inplace = True)
        query = text('SELECT * FROM weaknesses_junction;')
        r = conn.execute(query)
        weaknesses_junction_db = pd.DataFrame(r)

        if len(weaknesses_junction_db):
            new_weaknesses_junction = weakness_junction_df[~weakness_junction_df.apply(lambda row: all(row[col] in weaknesses_junction_db[col].values for col in weaknesses_junction_db.columns), axis=1)]
        else:
            new_weaknesses_junction = weakness_junction_df
        try:
            new_weaknesses_junction.to_sql(name = 'weaknesses_junction', con = self.engine, if_exists = 'append', index = False)
        except Exception as e:
            print(f"Error: {str(e)}")
        
        conn.close()

    def technologies(self, technologies_df):
        print(technologies_df)
        conn = self.engine.connect()
        query = text('SELECT * FROM technologies;')
        r = conn.execute(query)
        technologies_db = pd.DataFrame(r)
        technologies_df.drop(columns = {'score'}, inplace = True)
        if len(technologies_db):
            new_technologies = technologies_df.loc[~technologies_df['technology'].isin(technologies_db['technology']), :]
        else:
            new_technologies = technologies_df
        try:
            new_technologies.to_sql(name = 'technologies', con = self.engine, if_exists = 'append', index = False)
        except Exception as e:
            print(f"Error: {str(e)}")
        
        conn.close()