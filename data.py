# -*- coding: utf-8 -*-
"""
Created on Sat Oct  7 09:30:37 2023

@author: treha
"""

import redshift_connector
import pandas as pd


import json
class Connect:
    @staticmethod
    def redshift_connection( cluster_name: str):
        print("redshift_connection: Getting Redshift credentials")
        cluster_identifier = ""
        profile_name = ""
        if cluster_name == "ett-datalake":
            cluster_identifier = "eu-north-1-ett-datalake-data-platform-redshift"
            profile_name="nv-redshift-developer-ett"
        elif cluster_name == "testlake":
            cluster_identifier = "eu-north-1-aws-datalake-data-platform-redshift"
            profile_name="nv-redshift-developer-datalake"
        else:
            exit("Error redshift_connection: Wrong cluster name provided, available options are datalake and testlake")
        
        conn = redshift_connector.connect(iam=True,
                                          database="datawarehouse",
                                          db_user="developer",
                                          profile=profile_name,
                                          cluster_identifier=cluster_identifier
                                          )
        conn.autocommit = True
        return conn
    
    @staticmethod
    def query_redshift(query: str, conn) -> pd.DataFrame:
        with conn.cursor() as cursor:
            print("query_redshift: Querying data")
            cursor.execute(query)
            return cursor.fetch_dataframe()
          
         
    @staticmethod
    def query(starting_date: str, end_date: str):
        query_sql= f"""
        SELECT
            nv_id,
            machine_fbs,
            measurements."defects"."value" AS defects,
            measurements
        FROM dbt_loading.default_process_results_unique
        WHERE kind = 'CellEolVisionInspection'
            AND started_at BETWEEN '{starting_date}' AND '{end_date}'
        ORDER BY started_at DESC
        LIMIT 100
        """
        return query_sql
        
    
        
 
    @staticmethod    
    def convert_to_numbers(df):
        
      
        excel = pd.DataFrame({})

         


         

        # Check the updated dataframe

        #print(df['Unnamed: 1'])



        df["defects"].fillna(101, inplace = True)
        cell_IDs = df["nv_id"]


        #df = pd.read_csv("defects.csv", delimiter=',' , on_bad_lines= 'skip')

        print('startwd')

        # Convert strings to dictionaries





        # Explode dictionaries column into multiple columns

         

        # Join ID column with data columns

        #df = df[["id"]].join(df2)


        back = []

        front = []

        bottom = []

        short_sides = []

        ind = 0




        for j in range(len(cell_IDs)):

         

            if df["defects"][j] == 101:

                continue

           
            
            first = df["defects"][j]

            json_object = json.loads(first)

         

            for i in range(len(json_object)):

                if json_object[i]["class"] == "BubbleParticle":

                   

                    if json_object[i]["side"] == "Back":

                        back.append(json_object[i]["number"])


                    elif json_object[i]["side"] == "Front":

                        front.append(json_object[i]["number"])

         

                    elif json_object[i]["side"] == "Bottom":

                        bottom.append(json_object[i]["number"])

                    #print(df["id"][j] + ' Height', json_object[i]["heightInMm"], json_object[i]["class"])

                    elif json_object[i]["side"] == "Left" or json_object[i]["side"] == "Right":

                        short_sides.append(json_object[i]["number"])
                        
              
           

           

            if not(back):

                back.append(0)

            if not(front):

                front.append(0)

            if not(bottom):

                bottom.append(0)

            if not(short_sides):

                short_sides.append(0)

               

               
            add = pd.DataFrame({ 'Cell ID': cell_IDs[j] , 'Back': max(back), 'Front': max(front), 'Bottom': max(bottom), 'Short Sides': max(short_sides)}, index=[ind])
         

            excel = pd.concat([excel,add ])

           

         

           

            back = []

            front = []

            bottom = []

            short_sides= []

            ind +=1
            
        
        
        return excel