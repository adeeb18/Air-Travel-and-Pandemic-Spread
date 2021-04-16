const express = require("express");
const http = require("http");
const socketIo = require("socket.io");

const port = process.env.PORT || 5000;
const index = require("./routes/index");

const app = express();
app.use(index);


const server = http.createServer(app);

const io = socketIo(server, {
  cors: {
    origin: "http://localhost:3000",
    methods: ["GET", "POST"],
    credentials: true
  }
});
const oracledb = require('oracledb');

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
try {
  oracledb.initOracleClient({libDir:
        'C:\\Users\\taylo\\OneDrive\\Documents\\Path\\instantclient_19_10'});
} catch(err) {
  console.error(err);
  console.error(1);
  process.exit(1);
}

io.on("connection", (socket) => {
  console.log("New client connected");
  socket.on('Query 1',(arguments) => {
    console.log(arguments);

// Instant client handler. Verifies that instant client can be found

    async function run(arguments) {
      let connection;

      try {
        connection = await oracledb.getConnection( {
          user : "adeeb.rashid",
          password : "zachiscute123",
          connectString : "oracle.cise.ufl.edu/orcl"
        });

        const result = await connection.execute(
          `SELECT D1 AS X, NVL(N1,0) N1, NVL(DECODE(SIGN(ND1),-1,0,ND1),0) ND1, NVL(N2,0) as N2, NVL(DECODE(SIGN(ND2),-1,0,ND2),0) ND2, NVL(N3,0) N3, NVL(DECODE(SIGN(ND3),-1,0,ND3),0) ND3, NVL(DBP1,0) DBP1, NVL(DBP2,0) DBP2, NVL(DBP3, 0) DBP3, NVL(S1, 0) S1,NVL(S2, 0) S2,NVL(S3, 0) S3,NVL(A1, 0) A1,NVL(A2,0) A2,NVL(A3,0) A3,NVL(HW1,0) HW1,NVL(HW2,0) HW2,NVL(HW3,0) HW3
          FROM
            (SELECT DAY D1, NEW_CASES N1, NEW_DEATHS ND1
            FROM PROJECT_STATISTIC2020
            WHERE COUNTRY_NAME = '`+ arguments[0] + `')
            FULL OUTER JOIN
            (SELECT DAY D2, NEW_CASES N2, NEW_DEATHS ND2
            FROM PROJECT_STATISTIC2020
            WHERE COUNTRY_NAME = '`+ arguments[1] + `')
            ON D1 = D2
            FULL OUTER JOIN
            (SELECT DAY D3, NEW_CASES N3, NEW_DEATHS ND3
            FROM PROJECT_STATISTIC2020
            WHERE COUNTRY_NAME = '`+ arguments[2] + `')
            ON D1 = D3
            FULL OUTER JOIN
            (SELECT DAY DDBP1, DIABETES_PREVALENCE DBP1
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[0] + `')
            ON D1 = DDBP1
            FULL OUTER JOIN
            (SELECT DAY DDBP2, DIABETES_PREVALENCE DBP2
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[1] + `')
            ON D1 = DDBP2
            FULL OUTER JOIN
            (SELECT DAY DDBP3, DIABETES_PREVALENCE DBP3
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[2] + `')
            ON D1 = DDBP3
            FULL OUTER JOIN
            (SELECT DAY DS1, FEMALE_SMOKERS + MALE_SMOKERS S1
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[0] + `')
            ON D1 = DS1
            FULL OUTER JOIN
            (SELECT DAY DS2, FEMALE_SMOKERS + MALE_SMOKERS S2
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[1] + `')
            ON D1 = DS2
            FULL OUTER JOIN
            (SELECT DAY DS3, FEMALE_SMOKERS + MALE_SMOKERS S3
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[2] + `')
            ON D1 = DS3
            FULL OUTER JOIN
            (SELECT DAY DA1, AGED_65_OR_OLDER A1
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[0] + `')
            ON D1 = DA1
            FULL OUTER JOIN
            (SELECT DAY DA2, AGED_65_OR_OLDER A2
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[1] + `')
            ON D1 = DA2
            FULL OUTER JOIN
            (SELECT DAY DA3, AGED_65_OR_OLDER A3
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[2] + `')
            ON D1 = DA3
            FULL OUTER JOIN
            (SELECT DAY DHW1, HANDWASHING_FACILITIES HW1
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[0] + `')
            ON D1 = DHW1
            FULL OUTER JOIN
            (SELECT DAY DHW2, HANDWASHING_FACILITIES HW2
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[1] + `')
            ON D1 = DHW2
            FULL OUTER JOIN
            (SELECT DAY DHW3, HANDWASHING_FACILITIES HW3
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[2] + `')
            ON D1 = DHW3
            WHERE D1 IS NOT NULL
            ORDER BY D1`
        );
        console.log(result.rows);
        return result.rows;
      } catch(err) {
        console.error(err);
      } finally {
        if(connection) {
          try {
            await connection.close();
          } catch(err) {
            console.error(err);
          }
        }
      }
    }

    let se= run(arguments);
    se.then(value => io.emit("Query 1",value));
  });




  socket.on('Query 2',(arguments) => {
    console.log(arguments);
    // Instant client handler. Verifies that instant client can be found
    async function run(arguments) {
      let connection;

      try {
        connection = await oracledb.getConnection( {
          user : "adeeb.rashid",
          password : "zachiscute123",
          connectString : "oracle.cise.ufl.edu/orcl"
        });

        const result = await connection.execute(
            `SELECT X1, Y1, Y2, Y3, Y4 FROM
            (
            SELECT M1 AS X1, C1+C2 AS Y1 FROM
            (SELECT EXTRACT(MONTH FROM END_TIME) M1, A1.COUNTRY, COUNT(*) C1
            FROM "PROJECT_FLIGHT" JOIN "PROJECT_AIRPORT" A1 ON DEPARTURE = A1.ICAOAP
            WHERE A1.COUNTRY = '` + arguments[0] + `'
            GROUP  BY  EXTRACT(MONTH FROM END_TIME), A1.COUNTRY
            ORDER BY EXTRACT(MONTH FROM END_TIME)
            ) JOIN
            (
            SELECT EXTRACT(MONTH FROM END_TIME) M2, A2.COUNTRY, COUNT(*) C2
            FROM "PROJECT_FLIGHT" JOIN "PROJECT_AIRPORT" A2 ON DESTINATION = A2.ICAOAP
            WHERE A2.COUNTRY = '` + arguments[0] + `'
            GROUP  BY  EXTRACT(MONTH FROM END_TIME), A2.COUNTRY
            ORDER BY EXTRACT(MONTH FROM END_TIME)
            )
            ON M1 = M2
            )
            NATURAL JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) AS X1, SUM(NEW_CASES) AS Y2
            FROM "PROJECT_STATISTIC2020"
            WHERE COUNTRY_NAME = '` + arguments[0] + `'
            GROUP  BY  EXTRACT(MONTH FROM DAY)
            ORDER BY EXTRACT(MONTH FROM DAY)
            )
            JOIN
            (
            SELECT M1 AS X2, ROUND(CNT1+CNT2, 4) AS Y3 FROM
            (
            SELECT EXTRACT(MONTH FROM D2) M2, (COUNT(*)*13800/P2) CNT2
            FROM(
            SELECT END_TIME D2, PC2.POPULATION P2, A2.COUNTRY
            FROM "PROJECT_FLIGHT" JOIN "PROJECT_AIRPORT" A2 ON DESTINATION = A2.ICAOAP JOIN "PROJECT_COUNTRY" PC2 ON A2.COUNTRY = PC2.NAME
            WHERE A2.COUNTRY = '` + arguments[0] + `'
            )
            GROUP BY EXTRACT(MONTH FROM D2), P2
            ORDER BY EXTRACT(MONTH FROM D2)
            )
            JOIN
            (
            SELECT EXTRACT(MONTH FROM D1) M1, (COUNT(*)*13800/P1) CNT1
            FROM(
            SELECT END_TIME D1, PC1.POPULATION P1, A1.COUNTRY
            FROM "PROJECT_FLIGHT" JOIN "PROJECT_AIRPORT" A1 ON DEPARTURE = A1.ICAOAP JOIN "PROJECT_COUNTRY" PC1 ON A1.COUNTRY = PC1.NAME
            WHERE A1.COUNTRY = '` + arguments[0] + `'
            )
            GROUP BY EXTRACT(MONTH FROM D1), P1
            ORDER BY EXTRACT(MONTH FROM D1)
            )
            ON M1 = M2
            )
            ON X1 = X2
            NATURAL JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) AS X2, ROUND(100 *SUM(NEW_CASES)/ (SELECT POPULATION FROM "PROJECT_COUNTRY" WHERE NAME = '` + arguments[0] + `'), 8) AS Y4
            FROM "PROJECT_STATISTIC2020"
            WHERE COUNTRY_NAME = '` + arguments[0] + `'
            GROUP  BY  EXTRACT(MONTH FROM DAY)
            ORDER BY EXTRACT(MONTH FROM DAY)
            )`
        );
        console.log(result.rows);
        return result.rows;
      } catch(err) {
        console.error(err);
      } finally {
        if(connection) {
          try {
            await connection.close();
          } catch(err) {
            console.error(err);
          }
        }
      }
    }

    let se= run(arguments);
    se.then(value => io.emit("Query 2",value));
  });




  socket.on('Query 3',(arguments) => {
    console.log(arguments);
    async function run(arguments) {
      let connection;

      try {
        connection = await oracledb.getConnection( {
          user : "adeeb.rashid",
          password : "zachiscute123",
          connectString : "oracle.cise.ufl.edu/orcl"
        });

        const result = await connection.execute(
          `SELECT X,
          NVL(Y1, 0) FLIGHTS,
          NVL(Y2, 0) CASES,
          NVL(Y3, 0) BRAZIL,
          NVL(Y4, 0) CHINA,
          NVL(Y5, 0) INDIA,
          NVL(Y6, 0) UNITEDSTATES
          FROM (
            SELECT EXTRACT(MONTH FROM END_TIME) AS X, COUNT(*) AS Y1
            FROM PROJECT_FLIGHT
            WHERE DEPARTURE IN
                  (SELECT ICAOAP
                   FROM PROJECT_AIRPORT
                   WHERE COUNTRY IN
                         (
                             SELECT DISTINCT COUNTRY_NAME AS HOTSPOTS
                             FROM (
                                      SELECT COUNTRY_NAME,
                                             MONTH,
                                             SUMS,
                                             ROW_NUMBER() OVER (PARTITION BY MONTH ORDER BY SUMS DESC) AS RN
                                      FROM (
                                               SELECT COUNTRY_NAME,
                                                      EXTRACT(MONTH FROM DAY) AS MONTH,
                                                      SUM(NEW_CASES)          AS SUMS
                                               FROM PROJECT_STATISTIC2020
                                               WHERE COUNTRY_NAME NOT IN ('World', 'International', 'Europe', 'North America', 'Asia', 'Africa', 'North America', 'European Union', 'South America')
                                                 AND NEW_CASES IS NOT NULL
                                               GROUP BY COUNTRY_NAME, EXTRACT(MONTH FROM DAY)
                                               ORDER BY MONTH DESC, SUM(NEW_CASES) DESC
                                           )
                                  )
                             WHERE RN <= 1
                         )
                  )
              AND DESTINATION IN
                  (SELECT ICAOAP
                   FROM PROJECT_AIRPORT
                   WHERE COUNTRY = '`+ arguments[0] +`'
                  )
            GROUP BY EXTRACT(MONTH FROM END_TIME)
            ORDER BY EXTRACT(MONTH FROM END_TIME)
        )
            NATURAL JOIN
        (
            SELECT EXTRACT(MONTH FROM DAY) AS X, SUM(NEW_CASES) AS Y2
            FROM PROJECT_STATISTIC2020
            WHERE COUNTRY_NAME = '`+ arguments[0] +`'
     
            GROUP BY EXTRACT(MONTH FROM DAY)
            ORDER BY EXTRACT(MONTH FROM DAY)
        )
            FULL OUTER JOIN
        (
            SELECT EXTRACT(MONTH FROM END_TIME) AS X1, COUNT(*) AS Y3
            FROM PROJECT_FLIGHT
            WHERE DEPARTURE IN
                  (SELECT ICAOAP
                   FROM PROJECT_AIRPORT
                   WHERE COUNTRY IN
                         (
                             SELECT HOTSPOTS
                             FROM (
                                      SELECT HOTSPOTS, ROW_NUMBER() OVER (ORDER BY HOTSPOTS) ROWTOBECHANGED
                                      FROM (
                                               SELECT DISTINCT COUNTRY_NAME AS HOTSPOTS
                                               FROM (
                                                        SELECT COUNTRY_NAME,
                                                               MONTH,
                                                               SUMS,
                                                               ROW_NUMBER() OVER (PARTITION BY MONTH ORDER BY SUMS DESC) AS RN
                                                        FROM (
                                                                 SELECT COUNTRY_NAME,
                                                                        EXTRACT(MONTH FROM DAY) AS MONTH,
                                                                        SUM(NEW_CASES)          AS SUMS
                                                                 FROM PROJECT_STATISTIC2020
                                                                 WHERE COUNTRY_NAME NOT IN ('World', 'International', 'Europe', 'North America', 'Asia', 'Africa', 'North America', 'European Union', 'South America')
                                                                   AND NEW_CASES IS NOT NULL
                                                                 GROUP BY COUNTRY_NAME, EXTRACT(MONTH FROM DAY)
                                                                 ORDER BY MONTH DESC, SUM(NEW_CASES) DESC
                                                             )
                                                    )
                                               WHERE RN <= 1
                                           )
                                  )
                             WHERE ROWTOBECHANGED = 1
                         )
                  )
              AND DESTINATION IN
                  (SELECT ICAOAP
                   FROM PROJECT_AIRPORT
                   WHERE COUNTRY = '`+ arguments[0] +`'
                  )
            GROUP BY EXTRACT(MONTH FROM END_TIME)
            ORDER BY EXTRACT(MONTH FROM END_TIME)
        )
        ON X = X1
            FULL OUTER JOIN
        (
            SELECT EXTRACT(MONTH FROM END_TIME) AS X2, COUNT(*) AS Y4
            FROM PROJECT_FLIGHT
            WHERE DEPARTURE IN
                  (SELECT ICAOAP
                   FROM PROJECT_AIRPORT
                   WHERE COUNTRY IN
                         (
                             SELECT HOTSPOTS
                             FROM (
                                      SELECT HOTSPOTS, ROW_NUMBER() OVER (ORDER BY HOTSPOTS) ROWTOBECHANGED
                                      FROM (
                                               SELECT DISTINCT COUNTRY_NAME AS HOTSPOTS
                                               FROM (
                                                        SELECT COUNTRY_NAME,
                                                               MONTH,
                                                               SUMS,
                                                               ROW_NUMBER() OVER (PARTITION BY MONTH ORDER BY SUMS DESC) AS RN
                                                        FROM (
                                                                 SELECT COUNTRY_NAME,
                                                                        EXTRACT(MONTH FROM DAY) AS MONTH,
                                                                        SUM(NEW_CASES)          AS SUMS
                                                                 FROM PROJECT_STATISTIC2020
                                                                 WHERE COUNTRY_NAME NOT IN ('World', 'International', 'Europe', 'North America', 'Asia', 'Africa', 'North America', 'European Union', 'South America')
                                                                   AND NEW_CASES IS NOT NULL
                                                                 GROUP BY COUNTRY_NAME, EXTRACT(MONTH FROM DAY)
                                                                 ORDER BY MONTH DESC, SUM(NEW_CASES) DESC
                                                             )
                                                    )
                                               WHERE RN <= 1
                                           )
                                  )
                             WHERE ROWTOBECHANGED = 2
                         )
                  )
              AND DESTINATION IN
                  (SELECT ICAOAP
                   FROM PROJECT_AIRPORT
                   WHERE COUNTRY = '`+ arguments[0] +`'
                  )
            GROUP BY EXTRACT(MONTH FROM END_TIME)
            ORDER BY EXTRACT(MONTH FROM END_TIME)
        )
        ON X2 = X
            FULL OUTER JOIN
        (
            SELECT EXTRACT(MONTH FROM END_TIME) AS X3, COUNT(*) AS Y5
            FROM PROJECT_FLIGHT
            WHERE DEPARTURE IN
                  (SELECT ICAOAP
                   FROM PROJECT_AIRPORT
                   WHERE COUNTRY IN
                         (
                             SELECT HOTSPOTS
                             FROM (
                                      SELECT HOTSPOTS, ROW_NUMBER() OVER (ORDER BY HOTSPOTS) ROWTOBECHANGED
                                      FROM (
                                               SELECT DISTINCT COUNTRY_NAME AS HOTSPOTS
                                               FROM (
                                                        SELECT COUNTRY_NAME,
                                                               MONTH,
                                                               SUMS,
                                                               ROW_NUMBER() OVER (PARTITION BY MONTH ORDER BY SUMS DESC) AS RN
                                                        FROM (
                                                                 SELECT COUNTRY_NAME,
                                                                        EXTRACT(MONTH FROM DAY) AS MONTH,
                                                                        SUM(NEW_CASES)          AS SUMS
                                                                 FROM PROJECT_STATISTIC2020
                                                                 WHERE COUNTRY_NAME NOT IN ('World', 'International', 'Europe', 'North America', 'Asia', 'Africa', 'North America', 'European Union', 'South America')
                                                                   AND NEW_CASES IS NOT NULL
                                                                 GROUP BY COUNTRY_NAME, EXTRACT(MONTH FROM DAY)
                                                                 ORDER BY MONTH DESC, SUM(NEW_CASES) DESC
                                                             )
                                                    )
                                               WHERE RN <= 1
                                           )
                                  )
                             WHERE ROWTOBECHANGED = 3
                         )
                  )
              AND DESTINATION IN
                  (SELECT ICAOAP
                   FROM PROJECT_AIRPORT
                   WHERE COUNTRY = '`+ arguments[0] +`'
                  )
            GROUP BY EXTRACT(MONTH FROM END_TIME)
            ORDER BY EXTRACT(MONTH FROM END_TIME)
        )
        ON X3 = X
            FULL OUTER JOIN
        (
            SELECT EXTRACT(MONTH FROM END_TIME) AS X4, COUNT(*) AS Y6
            FROM PROJECT_FLIGHT
            WHERE DEPARTURE IN
                  (SELECT ICAOAP
                   FROM PROJECT_AIRPORT
                   WHERE COUNTRY IN
                         (
                             SELECT HOTSPOTS
                             FROM (
                                      SELECT HOTSPOTS, ROW_NUMBER() OVER (ORDER BY HOTSPOTS) ROWTOBECHANGED
                                      FROM (
                                               SELECT DISTINCT COUNTRY_NAME AS HOTSPOTS
                                               FROM (
                                                        SELECT COUNTRY_NAME,
                                                               MONTH,
                                                               SUMS,
                                                               ROW_NUMBER() OVER (PARTITION BY MONTH ORDER BY SUMS DESC) AS RN
                                                        FROM (
                                                                 SELECT COUNTRY_NAME,
                                                                        EXTRACT(MONTH FROM DAY) AS MONTH,
                                                                        SUM(NEW_CASES)          AS SUMS
                                                                 FROM PROJECT_STATISTIC2020
                                                                 WHERE COUNTRY_NAME NOT IN ('World', 'International', 'Europe', 'North America', 'Asia', 'Africa', 'North America', 'European Union', 'South America')
                                                                   AND NEW_CASES IS NOT NULL
                                                                 GROUP BY COUNTRY_NAME, EXTRACT(MONTH FROM DAY)
                                                                 ORDER BY MONTH DESC, SUM(NEW_CASES) DESC
                                                             )
                                                    )
                                               WHERE RN <= 1
                                           )
                                  )
                             WHERE ROWTOBECHANGED = 4
                         )
                  )
              AND DESTINATION IN
                  (SELECT ICAOAP
                   FROM PROJECT_AIRPORT
                   WHERE COUNTRY = '`+ arguments[0] +`'
                  )
            GROUP BY EXTRACT(MONTH FROM END_TIME)
            ORDER BY EXTRACT(MONTH FROM END_TIME)
        )
        ON X4 = X`
        );
        console.log(result.rows);
        return result.rows;
      } catch(err) {
        console.error(err);
      } finally {
        if(connection) {
          try {
            await connection.close();
          } catch(err) {
            console.error(err);
          }
        }
      }
    }

    let se= run(arguments);
    se.then(value => io.emit("Query 3",value));
  });




  socket.on('Query 4',(arguments) => {
    console.log(arguments);
    async function run(arguments) {
      let connection;

      try {
        connection = await oracledb.getConnection( {
          user : "adeeb.rashid",
          password : "zachiscute123",
          connectString : "oracle.cise.ufl.edu/orcl"
        });

        const result = await connection.execute(
            `SELECT X1, Y1, Y2, Y3
            FROM
            (
            SELECT X1, ROUND(C/
            (SELECT MAX(C) FROM
            (SELECT MONTH X1, COUNT(*) C FROM(
            SELECT EXTRACT(MONTH FROM END_TIME) AS MONTH ,SUBSTR(CALLSIGN,1,3) AS ICAO
            FROM PROJECT_FLIGHT
            )
            WHERE ICAO IN (SELECT ICAOAL FROM PROJECT_AIRLINE WHERE NAME = '`+ arguments[0] +`')
            GROUP BY MONTH
            ORDER BY MONTH)
            ),4) Y1
            FROM (
            SELECT MONTH X1, COUNT(*) C FROM(
            SELECT EXTRACT(MONTH FROM END_TIME) AS MONTH ,SUBSTR(CALLSIGN,1,3) AS ICAO
            FROM PROJECT_FLIGHT
            )
            WHERE ICAO IN (SELECT ICAOAL FROM PROJECT_AIRLINE WHERE NAME = '`+ arguments[0] +`')
            GROUP BY MONTH
            ORDER BY MONTH)
            )
            JOIN
            (
            SELECT X2, ROUND(C/
            (SELECT MAX(C) FROM
            (SELECT MONTH X2, COUNT(*) C FROM(
            SELECT EXTRACT(MONTH FROM END_TIME) AS MONTH ,SUBSTR(CALLSIGN,1,3) AS ICAO
            FROM PROJECT_FLIGHT
            )
            WHERE ICAO IN (SELECT ICAOAL FROM PROJECT_AIRLINE WHERE NAME = '`+ arguments[1] +`')
            GROUP BY MONTH
            ORDER BY MONTH)
            ),4) Y2
            FROM (
            SELECT MONTH X2, COUNT(*) C FROM(
            SELECT EXTRACT(MONTH FROM END_TIME) AS MONTH ,SUBSTR(CALLSIGN,1,3) AS ICAO
            FROM PROJECT_FLIGHT
            )
            WHERE ICAO IN (SELECT ICAOAL FROM PROJECT_AIRLINE WHERE NAME = '`+ arguments[1] +`')
            GROUP BY MONTH
            ORDER BY MONTH)
            )
            ON X1 = X2
            JOIN
            (
            SELECT X3, ROUND(C/
            (SELECT MAX(C) FROM
            (SELECT MONTH X3, COUNT(*) C FROM(
            SELECT EXTRACT(MONTH FROM END_TIME) AS MONTH ,SUBSTR(CALLSIGN,1,3) AS ICAO
            FROM PROJECT_FLIGHT
            )
            WHERE ICAO IN (SELECT ICAOAL FROM PROJECT_AIRLINE WHERE NAME = '`+ arguments[2] +`')
            GROUP BY MONTH
            ORDER BY MONTH)
            ),4) Y3
            FROM (
            SELECT MONTH X3, COUNT(*) C FROM(
            SELECT EXTRACT(MONTH FROM END_TIME) AS MONTH ,SUBSTR(CALLSIGN,1,3) AS ICAO
            FROM PROJECT_FLIGHT
            )
            WHERE ICAO IN (SELECT ICAOAL FROM PROJECT_AIRLINE WHERE NAME = '`+ arguments[2] +`')
            GROUP BY MONTH
            ORDER BY MONTH)
            )
            ON X2 = X3`
        );
        console.log(result.rows);
        return result.rows;
      } catch(err) {
        console.error(err);
      } finally {
        if(connection) {
          try {
            await connection.close();
          } catch(err) {
            console.error(err);
          }
        }
      }
    }

    let se= run(arguments);
    se.then(value => io.emit("Query 4",value));
  });




  socket.on('Query 5',(arguments) => {
    console.log(arguments);
    async function run(arguments) {
      let connection;

      try {
        connection = await oracledb.getConnection( {
          user : "adeeb.rashid",
          password : "zachiscute123",
          connectString : "oracle.cise.ufl.edu/orcl"
        });

        const result = await connection.execute(
            `SELECT X, NVL(C,0) C, NVL(L1,0) L1, NVL(L2,0) L2, NVL(L3,0) L3, NVL(H1,0) H1, NVL(H2,0) H2, NVL(H3,0) H3, NVL(CS,0) CS, NVL(L1S,0) L1S, NVL(L2S,0) L2S , NVL(L3S,0) L3S, NVL(H1S,0) H1S, NVL(H2S,0) H2S , NVL(H3S,0) H3S FROM
            (
            SELECT EXTRACT(MONTH FROM DAY) X, ROUND(SUM((NEW_DEATHS)*1000000/POPULATION), 6) C
            FROM PROJECT_STATISTIC2020 
            JOIN PROJECT_COUNTRY 
            ON COUNTRY_NAME = NAME
            WHERE COUNTRY_NAME = '`+ arguments[0] +`'
            GROUP  BY  EXTRACT(MONTH FROM DAY), POPULATION
            ORDER BY EXTRACT(MONTH FROM DAY)
            )
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XL1, ROUND(SUM((NEW_DEATHS)*1000000/POPULATION),6) L1
            FROM PROJECT_STATISTIC2020 
            JOIN PROJECT_COUNTRY 
            ON COUNTRY_NAME = NAME
            WHERE COUNTRY_NAME = (
            SELECT LEASTSTRIN FROM(
            SELECT COUNTRY_NAME LEASTSTRIN, ROW_NUMBER() OVER (ORDER BY S) RN FROM(
            SELECT COUNTRY_NAME, ROUND(AVG(STRINGENCY_INDEX) , 4) S
            FROM(
            SELECT COUNTRY_NAME, STRINGENCY_INDEX
            FROM PROJECT_STATISTICNEW
            WHERE STRINGENCY_INDEX IS NOT NULL
            )
            GROUP BY COUNTRY_NAME
            ORDER BY S
            )
            WHERE ROWNUM < 4)
            WHERE RN = 1
            )
            GROUP  BY  EXTRACT(MONTH FROM DAY), POPULATION
            ORDER BY EXTRACT(MONTH FROM DAY)
            )
            ON X = XL1
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XL2, ROUND(SUM((NEW_DEATHS)*1000000/POPULATION),6) L2
            FROM PROJECT_STATISTIC2020 
            JOIN PROJECT_COUNTRY 
            ON COUNTRY_NAME = NAME
            WHERE COUNTRY_NAME = (
            SELECT LEASTSTRIN FROM(
            SELECT COUNTRY_NAME LEASTSTRIN, ROW_NUMBER() OVER (ORDER BY S) RN FROM(
            SELECT COUNTRY_NAME, ROUND(AVG(STRINGENCY_INDEX) , 4) S
            FROM(
            SELECT COUNTRY_NAME, STRINGENCY_INDEX
            FROM PROJECT_STATISTICNEW
            WHERE STRINGENCY_INDEX IS NOT NULL
            )
            GROUP BY COUNTRY_NAME
            ORDER BY S
            )
            WHERE ROWNUM < 4)
            WHERE RN = 2
            )
            GROUP  BY  EXTRACT(MONTH FROM DAY), POPULATION
            ORDER BY EXTRACT(MONTH FROM DAY)
            )
            ON XL2 = X
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XL3, ROUND(SUM((NEW_DEATHS)*1000000/POPULATION),6) L3
            FROM PROJECT_STATISTIC2020 
            JOIN PROJECT_COUNTRY 
            ON COUNTRY_NAME = NAME
            WHERE COUNTRY_NAME = (
            SELECT LEASTSTRIN FROM(
            SELECT COUNTRY_NAME LEASTSTRIN, ROW_NUMBER() OVER (ORDER BY S) RN FROM(
            SELECT COUNTRY_NAME, ROUND(AVG(STRINGENCY_INDEX) , 4) S
            FROM(
            SELECT COUNTRY_NAME, STRINGENCY_INDEX
            FROM PROJECT_STATISTICNEW
            WHERE STRINGENCY_INDEX IS NOT NULL
            )
            GROUP BY COUNTRY_NAME
            ORDER BY S
            )
            WHERE ROWNUM < 4)
            WHERE RN = 3
            )
            GROUP  BY  EXTRACT(MONTH FROM DAY), POPULATION
            ORDER BY EXTRACT(MONTH FROM DAY)
            )
            ON X = XL3
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XH1, ROUND(SUM((NEW_DEATHS)*1000000/POPULATION),6) H1
            FROM PROJECT_STATISTIC2020 
            JOIN PROJECT_COUNTRY 
            ON COUNTRY_NAME = NAME
            WHERE COUNTRY_NAME = (
            SELECT MOSTSTRIN FROM(
            SELECT COUNTRY_NAME MOSTSTRIN, ROW_NUMBER() OVER (ORDER BY S) RN FROM(
            SELECT COUNTRY_NAME, ROUND(AVG(STRINGENCY_INDEX) , 4) S
            FROM(
            SELECT COUNTRY_NAME, STRINGENCY_INDEX
            FROM PROJECT_STATISTICNEW
            WHERE STRINGENCY_INDEX IS NOT NULL
            )
            GROUP BY COUNTRY_NAME
            ORDER BY S DESC
            )
            WHERE ROWNUM < 4)
            WHERE RN = 1
            )
            GROUP  BY  EXTRACT(MONTH FROM DAY), POPULATION
            ORDER BY EXTRACT(MONTH FROM DAY)
            )
            ON X = XH1
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XH2, ROUND(SUM((NEW_DEATHS)*1000000/POPULATION),6) H2
            FROM PROJECT_STATISTIC2020 
            JOIN PROJECT_COUNTRY 
            ON COUNTRY_NAME = NAME
            WHERE COUNTRY_NAME = (
            SELECT MOSTSTRIN FROM(
            SELECT COUNTRY_NAME MOSTSTRIN, ROW_NUMBER() OVER (ORDER BY S) RN FROM(
            SELECT COUNTRY_NAME, ROUND(AVG(STRINGENCY_INDEX) , 4) S
            FROM(
            SELECT COUNTRY_NAME, STRINGENCY_INDEX
            FROM PROJECT_STATISTICNEW
            WHERE STRINGENCY_INDEX IS NOT NULL
            )
            GROUP BY COUNTRY_NAME
            ORDER BY S DESC
            )
            WHERE ROWNUM < 4)
            WHERE RN = 2
            )
            GROUP  BY  EXTRACT(MONTH FROM DAY), POPULATION
            ORDER BY EXTRACT(MONTH FROM DAY)
            )
            ON X = XH2
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XH3, ROUND(SUM((NEW_DEATHS)*1000000/POPULATION),6) H3
            FROM PROJECT_STATISTIC2020 
            JOIN PROJECT_COUNTRY 
            ON COUNTRY_NAME = NAME
            WHERE COUNTRY_NAME = (
            SELECT MOSTSTRIN FROM(
            SELECT COUNTRY_NAME MOSTSTRIN, ROW_NUMBER() OVER (ORDER BY S) RN FROM(
            SELECT COUNTRY_NAME, ROUND(AVG(STRINGENCY_INDEX) , 4) S
            FROM(
            SELECT COUNTRY_NAME, STRINGENCY_INDEX
            FROM PROJECT_STATISTICNEW
            WHERE STRINGENCY_INDEX IS NOT NULL
            )
            GROUP BY COUNTRY_NAME
            ORDER BY S DESC
            )
            WHERE ROWNUM < 4)
            WHERE RN = 3
            )
            GROUP  BY  EXTRACT(MONTH FROM DAY), POPULATION
            ORDER BY EXTRACT(MONTH FROM DAY)
            )
            ON X = XH3
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XL1S, ROUND(AVG(STRINGENCY_INDEX), 6) L1S
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = (
            SELECT LEASTSTRIN FROM(
            SELECT COUNTRY_NAME LEASTSTRIN, ROW_NUMBER() OVER (ORDER BY S) RN FROM(
            SELECT COUNTRY_NAME, ROUND(AVG(STRINGENCY_INDEX) , 4) S
            FROM(
            SELECT COUNTRY_NAME, STRINGENCY_INDEX
            FROM PROJECT_STATISTICNEW
            WHERE STRINGENCY_INDEX IS NOT NULL
            )
            GROUP BY COUNTRY_NAME
            ORDER BY S
            )
            WHERE ROWNUM < 4)
            WHERE RN = 1
            )
            GROUP BY EXTRACT(MONTH FROM DAY)
            ORDER BY XL1S
            )
            ON XL1S = X
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XL2S, ROUND(AVG(STRINGENCY_INDEX), 6) L2S
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = (
            SELECT LEASTSTRIN FROM(
            SELECT COUNTRY_NAME LEASTSTRIN, ROW_NUMBER() OVER (ORDER BY S) RN FROM(
            SELECT COUNTRY_NAME, ROUND(AVG(STRINGENCY_INDEX) , 4) S
            FROM(
            SELECT COUNTRY_NAME, STRINGENCY_INDEX
            FROM PROJECT_STATISTICNEW
            WHERE STRINGENCY_INDEX IS NOT NULL
            )
            GROUP BY COUNTRY_NAME
            ORDER BY S
            )
            WHERE ROWNUM < 4)
            WHERE RN = 2
            )
            GROUP BY EXTRACT(MONTH FROM DAY)
            ORDER BY XL2S
            )
            ON XL2S = X
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XL3S, ROUND(AVG(STRINGENCY_INDEX), 6) L3S
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = (
            SELECT LEASTSTRIN FROM(
            SELECT COUNTRY_NAME LEASTSTRIN, ROW_NUMBER() OVER (ORDER BY S) RN FROM(
            SELECT COUNTRY_NAME, ROUND(AVG(STRINGENCY_INDEX) , 4) S
            FROM(
            SELECT COUNTRY_NAME, STRINGENCY_INDEX
            FROM PROJECT_STATISTICNEW
            WHERE STRINGENCY_INDEX IS NOT NULL
            )
            GROUP BY COUNTRY_NAME
            ORDER BY S
            )
            WHERE ROWNUM < 4)
            WHERE RN = 3
            )
            GROUP BY EXTRACT(MONTH FROM DAY)
            ORDER BY XL3S
            )
            ON XL3S = X
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XH1S, ROUND(AVG(STRINGENCY_INDEX), 6) H1S
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = (
            SELECT LEASTSTRIN FROM(
            SELECT COUNTRY_NAME LEASTSTRIN, ROW_NUMBER() OVER (ORDER BY S) RN FROM(
            SELECT COUNTRY_NAME, ROUND(AVG(STRINGENCY_INDEX) , 4) S
            FROM(
            SELECT COUNTRY_NAME, STRINGENCY_INDEX
            FROM PROJECT_STATISTICNEW
            WHERE STRINGENCY_INDEX IS NOT NULL
            )
            GROUP BY COUNTRY_NAME
            ORDER BY S DESC
            )
            WHERE ROWNUM < 4)
            WHERE RN = 1
            )
            GROUP BY EXTRACT(MONTH FROM DAY)
            ORDER BY XH1S
            )
            ON XH1S = X
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XH2S, ROUND(AVG(STRINGENCY_INDEX), 6) H2S
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = (
            SELECT LEASTSTRIN FROM(
            SELECT COUNTRY_NAME LEASTSTRIN, ROW_NUMBER() OVER (ORDER BY S) RN FROM(
            SELECT COUNTRY_NAME, ROUND(AVG(STRINGENCY_INDEX) , 4) S
            FROM(
            SELECT COUNTRY_NAME, STRINGENCY_INDEX
            FROM PROJECT_STATISTICNEW
            WHERE STRINGENCY_INDEX IS NOT NULL
            )
            GROUP BY COUNTRY_NAME
            ORDER BY S DESC
            )
            WHERE ROWNUM < 4)
            WHERE RN = 2
            )
            GROUP BY EXTRACT(MONTH FROM DAY)
            ORDER BY XH2S
            )
            ON XH2S = X
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XH3S, ROUND(AVG(STRINGENCY_INDEX), 6) H3S
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = (
            SELECT LEASTSTRIN FROM(
            SELECT COUNTRY_NAME LEASTSTRIN, ROW_NUMBER() OVER (ORDER BY S) RN FROM(
            SELECT COUNTRY_NAME, ROUND(AVG(STRINGENCY_INDEX) , 4) S
            FROM(
            SELECT COUNTRY_NAME, STRINGENCY_INDEX
            FROM PROJECT_STATISTICNEW
            WHERE STRINGENCY_INDEX IS NOT NULL
            )
            GROUP BY COUNTRY_NAME
            ORDER BY S DESC
            )
            WHERE ROWNUM < 4)
            WHERE RN = 3
            )
            GROUP BY EXTRACT(MONTH FROM DAY)
            ORDER BY XH3S
            )
            ON XH3S = X
            FULL OUTER JOIN
            (
            SELECT EXTRACT(MONTH FROM DAY) XCS, ROUND(AVG(STRINGENCY_INDEX), 6) CS
            FROM PROJECT_STATISTICNEW
            WHERE COUNTRY_NAME = '`+ arguments[0] +`'
            GROUP BY EXTRACT(MONTH FROM DAY)
            ORDER BY XCS
            )
            ON XCS = X
            ORDER BY X`
        );
        console.log(result.rows)
        return result.rows;
      } catch(err) {
        console.error(err);
      } finally {
        if(connection) {
          try {
            await connection.close();
          } catch(err) {
            console.error(err);
          }
        }
      }
    }

    let se= run(arguments);
    se.then(value => io.emit("Query 5",value));
  });
});

server.listen(port, () => console.log(`Listening on port ${port}`));