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
            `SELECT *
            FROM "ADEEB.RASHID"."PROJECT_FLIGHT"
            FETCH FIRST 10 ROWS ONLY`
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
    se.then(value => io.emit("Results",value));
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
          `SELECT X, NVL(Y1,0) FLIGHTS, NVL(Y2,0) CASES, NVL(Y3,0) BRAZIL, NVL(Y4,0) CHINA, NVL(Y5,0) INDIA, NVL(Y6,0) UNITEDSTATES
          FROM
          (
          SELECT EXTRACT(MONTH FROM END_TIME) AS X, COUNT(*) AS Y1
          FROM PROJECT_FLIGHT
          WHERE DEPARTURE  IN
          (SELECT ICAOAP FROM PROJECT_AIRPORT WHERE COUNTRY IN
          (
          SELECT DISTINCT COUNTRY_NAME AS HOTSPOTS FROM(
          SELECT COUNTRY_NAME, MONTH, SUMS, ROW_NUMBER() OVER(PARTITION BY MONTH ORDER BY SUMS DESC) AS RN
          FROM(
          SELECT COUNTRY_NAME, EXTRACT(MONTH FROM DAY) AS MONTH, SUM(NEW_CASES) AS SUMS
          FROM PROJECT_STATISTIC2020
          WHERE COUNTRY_NAME NOT IN ('World', 'International', 'Europe', 'North America', 'Asia', 'Africa', 'North America, 'European Union', 'South America') AND NEW_CASES IS NOT NULL
          GROUP BY COUNTRY_NAME, EXTRACT(MONTH FROM DAY)
          ORDER BY MONTH DESC, SUM(NEW_CASES) DESC
          )
          )
          WHERE RN <=1
          )
          )
          AND DESTINATION IN
          (SELECT ICAOAP FROM PROJECT_AIRPORT WHERE COUNTRY = '`+ arguments[0] +`'
          )
          GROUP  BY  EXTRACT(MONTH FROM END_TIME)
          ORDER BY EXTRACT(MONTH FROM END_TIME)
          )
          NATURAL JOIN
          (
          SELECT EXTRACT(MONTH FROM DAY) AS X, SUM(NEW_CASES) AS Y2
          FROM PROJECT_STATISTIC2020
          WHERE COUNTRY_NAME = '`+ arguments[0] +`'
          
          GROUP  BY  EXTRACT(MONTH FROM DAY)
          ORDER BY EXTRACT(MONTH FROM DAY)
          )
          FULL OUTER JOIN
          (
          SELECT EXTRACT(MONTH FROM END_TIME) AS X1, COUNT(*) AS Y3
          FROM PROJECT_FLIGHT
          WHERE DEPARTURE  IN
          (SELECT ICAOAP FROM PROJECT_AIRPORT WHERE COUNTRY IN
          (
          SELECT HOTSPOTS FROM(
          SELECT HOTSPOTS, ROW_NUMBER() OVER (ORDER BY HOTSPOTS) ROWTOBECHANGED FROM(
          SELECT DISTINCT COUNTRY_NAME AS HOTSPOTS FROM(
          SELECT COUNTRY_NAME, MONTH, SUMS, ROW_NUMBER() OVER(PARTITION BY MONTH ORDER BY SUMS DESC) AS RN
          FROM(
          SELECT COUNTRY_NAME, EXTRACT(MONTH FROM DAY) AS MONTH, SUM(NEW_CASES) AS SUMS
          FROM PROJECT_STATISTIC2020
          WHERE COUNTRY_NAME NOT IN ('World', 'International', 'Europe', 'North America', 'Asia', 'Africa', 'North America, 'European Union', 'South America') AND NEW_CASES IS NOT NULL
          GROUP BY COUNTRY_NAME, EXTRACT(MONTH FROM DAY)
          ORDER BY MONTH DESC, SUM(NEW_CASES) DESC
          )
          )
          WHERE RN <=1
          )
          )
          WHERE ROWTOBECHANGED = 1
          )
          )
          AND DESTINATION IN
          (SELECT ICAOAP FROM PROJECT_AIRPORT WHERE COUNTRY = '`+ arguments[0] +`'
          )
          GROUP  BY  EXTRACT(MONTH FROM END_TIME)
          ORDER BY EXTRACT(MONTH FROM END_TIME)
          )
          ON X = X1
          FULL OUTER JOIN
          (
          SELECT EXTRACT(MONTH FROM END_TIME) AS X2, COUNT(*) AS Y4
          FROM PROJECT_FLIGHT
          WHERE DEPARTURE  IN
          (SELECT ICAOAP FROM PROJECT_AIRPORT WHERE COUNTRY IN
          (
          SELECT HOTSPOTS FROM(
          SELECT HOTSPOTS, ROW_NUMBER() OVER (ORDER BY HOTSPOTS) ROWTOBECHANGED FROM(
          SELECT DISTINCT COUNTRY_NAME AS HOTSPOTS FROM(
          SELECT COUNTRY_NAME, MONTH, SUMS, ROW_NUMBER() OVER(PARTITION BY MONTH ORDER BY SUMS DESC) AS RN
          FROM(
          SELECT COUNTRY_NAME, EXTRACT(MONTH FROM DAY) AS MONTH, SUM(NEW_CASES) AS SUMS
          FROM PROJECT_STATISTIC2020
          WHERE COUNTRY_NAME NOT IN ('World', 'International', 'Europe', 'North America', 'Asia', 'Africa', 'North America, 'European Union', 'South America') AND NEW_CASES IS NOT NULL
          GROUP BY COUNTRY_NAME, EXTRACT(MONTH FROM DAY)
          ORDER BY MONTH DESC, SUM(NEW_CASES) DESC
          )
          )
          WHERE RN <=1
          )
          )
          WHERE ROWTOBECHANGED = 2
          )
          )
          AND DESTINATION IN
          (SELECT ICAOAP FROM PROJECT_AIRPORT WHERE COUNTRY = '`+ arguments[0] +`'
          )
          GROUP  BY  EXTRACT(MONTH FROM END_TIME)
          ORDER BY EXTRACT(MONTH FROM END_TIME)
          )
          ON X2 = X
          FULL OUTER JOIN
          (
          SELECT EXTRACT(MONTH FROM END_TIME) AS X3, COUNT(*) AS Y5
          FROM PROJECT_FLIGHT
          WHERE DEPARTURE  IN
          (SELECT ICAOAP FROM PROJECT_AIRPORT WHERE COUNTRY IN
          (
          SELECT HOTSPOTS FROM(
          SELECT HOTSPOTS, ROW_NUMBER() OVER (ORDER BY HOTSPOTS) ROWTOBECHANGED FROM(
          SELECT DISTINCT COUNTRY_NAME AS HOTSPOTS FROM(
          SELECT COUNTRY_NAME, MONTH, SUMS, ROW_NUMBER() OVER(PARTITION BY MONTH ORDER BY SUMS DESC) AS RN
          FROM(
          SELECT COUNTRY_NAME, EXTRACT(MONTH FROM DAY) AS MONTH, SUM(NEW_CASES) AS SUMS
          FROM PROJECT_STATISTIC2020
          WHERE COUNTRY_NAME NOT IN ('World', 'International', 'Europe', 'North America', 'Asia', 'Africa', 'North America, 'European Union', 'South America') AND NEW_CASES IS NOT NULL
          GROUP BY COUNTRY_NAME, EXTRACT(MONTH FROM DAY)
          ORDER BY MONTH DESC, SUM(NEW_CASES) DESC
          )
          )
          WHERE RN <=1
          )
          )
          WHERE ROWTOBECHANGED = 3
          )
          )
          AND DESTINATION IN
          (SELECT ICAOAP FROM PROJECT_AIRPORT WHERE COUNTRY = '`+ arguments[0] +`'
          )
          GROUP  BY  EXTRACT(MONTH FROM END_TIME)
          ORDER BY EXTRACT(MONTH FROM END_TIME)
          )
          ON X3 = X
          FULL OUTER JOIN
          (
          SELECT EXTRACT(MONTH FROM END_TIME) AS X4, COUNT(*) AS Y6
          FROM PROJECT_FLIGHT
          WHERE DEPARTURE  IN
          (SELECT ICAOAP FROM PROJECT_AIRPORT WHERE COUNTRY IN
          (
          SELECT HOTSPOTS FROM(
          SELECT HOTSPOTS, ROW_NUMBER() OVER (ORDER BY HOTSPOTS) ROWTOBECHANGED FROM(
          SELECT DISTINCT COUNTRY_NAME AS HOTSPOTS FROM(
          SELECT COUNTRY_NAME, MONTH, SUMS, ROW_NUMBER() OVER(PARTITION BY MONTH ORDER BY SUMS DESC) AS RN
          FROM(
          SELECT COUNTRY_NAME, EXTRACT(MONTH FROM DAY) AS MONTH, SUM(NEW_CASES) AS SUMS
          FROM PROJECT_STATISTIC2020
          WHERE COUNTRY_NAME NOT IN ('World', 'International', 'Europe', 'North America', 'Asia', 'Africa', 'North America, 'European Union', 'South America') AND NEW_CASES IS NOT NULL
          GROUP BY COUNTRY_NAME, EXTRACT(MONTH FROM DAY)
          ORDER BY MONTH DESC, SUM(NEW_CASES) DESC
          )
          )
          WHERE RN <=1
          )
          )
          WHERE ROWTOBECHANGED = 4
          )
          )
          AND DESTINATION IN
          (SELECT ICAOAP FROM PROJECT_AIRPORT WHERE COUNTRY = '`+ arguments[0] +`'
          )
          GROUP  BY  EXTRACT(MONTH FROM END_TIME)
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
            `SELECT *
            FROM "PROJECT_AIRPORT"
            FETCH FIRST 10 ROWS ONLY`
        );
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