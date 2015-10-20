DROP PACKAGE OPS$BDL.BTRIS_DATA_TRANSFER;

CREATE OR REPLACE PACKAGE OPS$BDL.BTRIS_DATA_TRANSFER AS
  /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
  /*     Author: Patrick Conrad - Ekagra Software Technologies                         */
  /*       Date: 11/17/11                                                              */
  /*Description: This process will get lab data from the external BTRIS Data Feed      */
  /*             and process the data prior to loading the lab data into Oracle        */
  /*             Clinical.  It DOES NOT load data to Oracle Clinical (C3D), but rather */
  /*             places it into the processing table for the Lab Loader.               */
  /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
  /*  Modification History                                                             */
  /* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
  /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

  -- Global Package Variables.

  Labs_Count      Number;

 
  Function PULL_NEW_LABS Return Number;
  Procedure PUSH_LABS_TO_LOADER;
  Procedure PULL_AND_PUSH_LABS(v_logName in Varchar2 Default NULL);
  Procedure Submit_PULL_AND_PUSH_LABS(JobNumber IN OUT NUMBER, v_logName IN OUT VARCHAR2);


  Function Text2Date(v_text in varchar2) return date;
  Function Date2NCIDate(v_Date in DATE) return Varchar2;

END BTRIS_DATA_TRANSFER;
/
DROP PACKAGE BODY OPS$BDL.BTRIS_DATA_TRANSFER;

CREATE OR REPLACE PACKAGE BODY OPS$BDL.BTRIS_DATA_TRANSFER
AS
 /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
 /*     Author: Patrick Conrad - Ekagra Software Technologies                         */
 /*       Date: 11/17/11                                                              */
 /*Description: This process will get lab data from the external BTRIS Data Feed      */
 /*             and process the data prior to loading the lab data into Oracle        */
 /*             Clinical.  It DOES NOT load data to Oracle Clinical (C3D), but rather */
 /*             places it into the processing table for the Lab Loader.               */
 /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
 /*  Modification History                                                             */
 /*  10/15/2014  Katie Grant  BTRIS Extract version 2.0                               */
 /*                           Added LAB_NOTE column to table BTRIS_LAB_TEST_RESULTS.  */
 /*                           Added the LAB_NOTE clause on the INSERT INTO            */
 /*                              BTRIS_LAB_RESULTS statement.                         */
 /* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
 /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

   FUNCTION Text2Date (v_text IN VARCHAR2)   RETURN DATE
   IS
  /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
  /*Description: This function is used to convert TEXT Dates to Oracle Dates         */
  /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
  /*  Modification History                                                           */
  /* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
  /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

      v_hold_text   VARCHAR2 (30);
      v_hold_date   DATE := NULL;
   BEGIN
      v_hold_text := v_text;

      IF INSTR (v_hold_text, '.') > 0
      THEN
         v_hold_text := SUBSTR (v_hold_text, 1, (INSTR (v_hold_text, '.') - 1));
      END IF;

      v_hold_date := TO_DATE (v_hold_text, 'YYYY-MM-DD HH24:MI:SS');

      RETURN v_hold_date;
   EXCEPTION
      WHEN OTHERS
      THEN
         Log_Util.LogMessage('TEXT2DATE ERROR: Unexpected ERROR Occurred in TEXT2DATE('||v_text|| ').');
         Log_Util.LogMessage ('TEXT2DATE ERROR: Error Encountered: ' || SQLCODE  );
         Log_Util.LogMessage ('TEXT2DATE ERROR: Error Message: ' || SQLERRM);
         RETURN NULL;
   END;

   FUNCTION Date2NCIDate (v_Date IN DATE)
      RETURN VARCHAR2
   IS
 /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
 /*Description: This function is converts a Date to a NCI Lab Loader Text Date        */
 /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
 /*  Modification History                                                             */
 /* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
 /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

      v_hold_date   DATE := NULL;
      v_hold_text   VARCHAR2 (30);
   BEGIN
      v_hold_date := v_date;
      v_hold_text := TO_CHAR (v_hold_date, 'MMDDYYHH24MI');
      RETURN v_hold_text;
   EXCEPTION
      WHEN OTHERS
      THEN
         Log_Util.LogMessage (
            'DATE2NCIDATE ERROR: Unexpected ERROR Occurred in DATE2NCIDATE.'    );
         Log_Util.LogMessage ( 'DATE2NCIDATE ERROR: Error Encountered: ' || SQLCODE );
         Log_Util.LogMessage ('DATE2NCIDATE ERROR: Error Message: ' || SQLERRM  );
         RETURN NULL;
   END;

   PROCEDURE PULL_AND_PUSH_LABS (v_logName IN VARCHAR2 DEFAULT NULL )
   IS
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/*Description: This Procedure is used to call the PULL_New_Labs function and then the*/
/*             PUSH_LABS_TO_LOADER procedure.  It allows for the LogName to be set at*/
/*             the time of execution.                                                */
/*-----------------------------------------------------------------------------------*/
/* Modification History:                                                             */
/*-----------------------------------------------------------------------------------*/
      JobText     VARCHAR2(300);
      v_NewRecs   NUMBER := 0;
      JobNumber   NUMBER;
   BEGIN
   
      IF v_logName IS NULL
      THEN
         Log_Util.LogSetName ('BTRIS_PULLPUSH_'||to_char(Sysdate,'YYYYMMDD-HH24MI'),'LABLOAD');
      ELSE
         Log_Util.LogSetName (v_logname, 'LABLOAD');
      END IF;
 
      Log_Util.LogMessage ('PAPL - Starting "BTRIS_DATA_TRANSFER.PULL_AND_PUSH_LABS".' );
      Log_Util.LogMessage ('PAPL - LogName = "' || v_logname || '".');

      Log_Util.LogMessage ('PAPL - Calling PULL_NEW_LABS.');
      v_NewRecs := PULL_NEW_LABS;

      Log_Util.LogMessage ('PAPL - Calling PUSH_LABS_TO_LOADER.');
      PUSH_LABS_TO_LOADER;

      Log_Util.LogMessage ('PAPL - Finished "BTRIS_DATA_TRANSFER.PULL_AND_PUSH_LABS".');
      
      BEGIN -- call Lab Loader job
         /*Log_Util.LogMessage('PAPL - About to submit the LabLoader job: CDW_DATA_TRANSFER_V3.GET_PROCESS_LOAD_LABS '|| 'Log name = '||v_logName);
         JobText := 'Begin CDW_DATA_TRANSFER_V3.GET_PROCESS_LOAD_LABS('''||v_logName||'''); End;';
         Log_Util.LogMessage (jobtext);
         DBMS_JOB.submit (job => JobNumber,
                          what => JobText,
                          next_date => SYSDATE,
                          interval => NULL); */
         Log_Util.LogMessage('PAPL - Calling the LabLoader job');
         CDW_DATA_TRANSFER_V3.GET_PROCESS_LOAD_LABS(v_logname,'PROCESS_AND_LOAD');
                          
      END;
   END;

   PROCEDURE Submit_PULL_AND_PUSH_LABS (JobNumber   IN OUT NUMBER,
                                        v_logName   IN OUT VARCHAR2)
   IS
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/*Description: This Procedure is used to SUBMIT as an Oracle Batch Job, the          */
/*             PULL_AND_PUSH_LABS procedure.                                         */
/*-----------------------------------------------------------------------------------*/
/* Modification History:                                                             */
/*-----------------------------------------------------------------------------------*/

      JobText   VARCHAR2 (200);
   BEGIN
      IF v_logName IS NULL
      THEN
         v_logName :='BTRIS_PULLPUSH_' || TO_CHAR (SYSDATE, 'YYYYMMDD-HH24MI');
      END IF;

      /*
      JobText := 'Begin BTRIS_DATA_TRANSFER.PULL_AND_PUSH_LABS('''|| v_logName|| '''); End;';

      DBMS_JOB.submit (job         => JobNumber,
                       what        => JobText,
                       next_date   => SYSDATE,
                       interval    => NULL); */
      BTRIS_DATA_TRANSFER.PULL_AND_PUSH_LABS(v_logName);    
   END;


   FUNCTION PULL_NEW_LABS  RETURN NUMBER
   IS
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/*Description: This function is used to pull records from the BTRIS Raw Lab FILE     */
/*             and place them into the BTRIS Lab Results Table in  preparation for   */
/*             loading to Oracle Clinical C3D.  The function only pulls those records*/
/*             that have not been pulled before, based on OBSERVATION GUID.          */
/*             'NEW' records are assigned a STATUS_FLAG of "N".  Function returns the*/
/*             count of NEW records.                                                 */
/*-----------------------------------------------------------------------------------*/
/* Modification History:                                   */
/*-----------------------------------------------------------------------------------*/

      v_rcount   NUMBER := 0;
   BEGIN
      BEGIN
         IF Log_Util.Log$LogName IS NULL
         THEN
            Log_Util.LogSetName ('BTRIS_TABLE_PULL_' || TO_CHAR (SYSDATE, 'YYYYMMDD-HH24MI'),'LABLOAD' );
         END IF;

         Log_Util.LogMessage ('PNL2 - Starting BTRIS_DATA_TRANSFER.PULL_NEW_LABS');
         Log_Util.LogMessage ('PNL2 - Starting INSERT Loop');

         INSERT INTO BTRIS_LAB_RESULTS (BTRIS_EVENT_ID,
                                        BTRIS_OBSERVATION_ID,
                                        BTRIS_LAB_TEST_ID,
                                        LAB_TEST_NAME,
                                        LAB_RESULT,
                                        LAB_UNIT,
                                        LAB_RANGE,
                                        LAB_DATE_TXT,
                                        BTRIS_SUBJECT_ID,
                                        PATIENT_ID,
                                        LABTEST_CODE,
                                        BTRIS_CREATE_DATE,
                                        BTRIS_MODIFY_DATE,
                                        LAB_REPORT_DATE,
                                        EXTRACT_QUERY_DATE,
                                        EXTRACT_EXTRACT_DATE,
                                        INSERT_DATE,
                                        STATUS_FLAG,
                                        LOAD_DATE,
                                        STATUS_TEXT,
                                        STATUS_DATE,
                                        LAB_NOTE,
                                        C3DAncestor)
            SELECT   BTRIS_EVENT_ID,
                     BTRIS_OBSERVATION_ID,
                     BTRIS_LAB_TEST_ID,
                     LAB_TEST_NAME,
                     LAB_RESULT,
                     LAB_UNIT,
                     LAB_RANGE,
                     LAB_DATE_TXT,
                     BTRIS_SUBJECT_ID,
                     PATIENT_ID,
                     LABTEST_CODE,
                     BTRIS_CREATE_DATE,
                     BTRIS_MODIFY_DATE,
                     LAB_REPORT_DATE,
                     EXTRACT_QUERY_DATE,
                     EXTRACT_EXTRACT_DATE,
                     SYSDATE,
                     'N',
                     NULL,
                     NULL,
                     NULL,
                     LAB_NOTE,
                     C3DAncestor
              FROM   BTRIS_LAB_TEST_RESULTS a
             WHERE   LAB_RESULT is not null
                     AND NOT EXISTS
                        (SELECT 'X'
                           FROM BTRIS_LAB_RESULTS c
                          WHERE a.BTRIS_OBSERVATION_ID = c.BTRIS_OBSERVATION_ID
                            AND a.BTRIS_EVENT_ID       = c.BTRIS_EVENT_ID
                            AND a.BTRIS_CREATE_DATE    = c.BTRIS_CREATE_DATE
                            AND NVL(a.BTRIS_MODIFY_DATE,'Z') = NVL(c.BTRIS_MODIFY_DATE,'Z'));

         v_rcount := SQL%ROWCOUNT;
         Log_Util.LogMessage('PNL2 - ' || TO_CHAR (v_rcount)
                             || ' rows inserted into "BTRIS_LAB_RESULTS" from "BTRIS_LAB_TEST_RESULTS".');
         COMMIT;
      END;

      Log_Util.LogMessage ('PNL2 - Finished BTRIS_DATA_TRANSFER.PULL_NEW_LABS' );

      RETURN v_Rcount;
   END PULL_NEW_LABS;

   PROCEDURE PUSH_LABS_TO_LOADER
   IS   
             
 BEGIN
      IF Log_Util.Log$LogName IS NULL
      THEN
         Log_Util.LogSetName ('BTRIS_PSH2LDR_' || TO_CHAR (SYSDATE, 'YYYYMMDD-HH24MI'),'LABLOAD' );
      END IF;
      Log_Util.LogMessage ('PLTL - Starting "BTRIS_DATA_TRANSFER.PUSH_LABS_TO_LOADER".' );
      BEGIN
      
  
        INSERT INTO NCI_LABS (PATIENT_ID,
                               SAMPLE_DATETIME,
                               TEST_COMPONENT_ID,
                               LABORATORY,
                               LOAD_FLAG,
                               LABTEST_NAME,
                               RESULT,
                               UNIT,
                               NORMAL_VALUE,
                               RECEIVED_DATE,
                               TEST_CODE,
                               CDW_RESULT_ID,
                               C3DAncestor)
            SELECT   PATIENT_ID,
                     Date2NCIDate (Text2Date (LAB_DATE_TXT)),
                     BTRIS_LAB_TEST_ID,
                     'BTRIS' LABORATORY,
                     'N' LOAD_FLAG,
                     LAB_TEST_NAME,
                     SUBSTRB (LAB_RESULT, 1, 200),   --NOTE the use of substrB
                     LAB_UNIT,
                     LAB_RANGE,
                     INSERT_DATE,
                     LABTEST_CODE,
                     BTRIS_OBSERVATION_ID,
                     C3DAncestor
              FROM   BTRIS_LAB_RESULTS a
             WHERE   STATUS_FLAG = 'N';

         Log_Util.LogMessage('PLTL - ' || TO_CHAR (SQL%ROWCOUNT)
                             || ' records inserted into "NCI_LABS" from "BTRIS_LAB_RESULTS".');

         UPDATE   BTRIS_LAB_RESULTS a
            SET   STATUS_FLAG = 'C',
                  STATUS_TEXT = 'Complete, moved to NCI_LABS.',
                  load_date = SYSDATE,
                  STATUS_DATE = SYSDATE
          WHERE   STATUS_FLAG = 'N';

         Log_Util.LogMessage('PLTL - ' || TO_CHAR (SQL%ROWCOUNT)
                             || ' BTRIS_LAB_RESULTS records updated STATUS_FLAG to "C".');
      EXCEPTION
         WHEN OTHERS
         THEN
            Log_Util.LogMessage('PLTL - Unexepected error during INSERT OR UPDATE in BTRIS_DATA_TRANSFER.PUSH_LABS_TO_LOADER.');
            Log_Util.LogMessage ('PLTL - Error Encountered: ' || SQLCODE);
            Log_Util.LogMessage ('PLTL - Error Message: ' || SQLERRM);
            RAISE;   -- stop processing, i.e. don't call LabLoader
      END;

      COMMIT;

      Log_Util.LogMessage ('PLTL - Finishing "PUSH_LABS_TO_LOADER".');
      
     
   END PUSH_LABS_TO_LOADER;
END BTRIS_DATA_TRANSFER;
/
