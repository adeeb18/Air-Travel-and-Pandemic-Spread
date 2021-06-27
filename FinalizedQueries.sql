--QUERY 1
--Cases/Deaths and Statistics

SELECT
    d1                                       AS x,
    n1,
    decode(sign(nd1), - 1, 0, nd1)           nd1,
    n2,
    decode(sign(nd2), - 1, 0, nd2)           nd2,
    n3,
    decode(sign(nd3), - 1, 0, nd3)           nd3,
    dbp1,
    dbp2,
    dbp3,
    s1,
    s2,
    s3,
    a1,
    a2,
    a3,
    hw1,
    hw2,
    hw3
FROM
    (
        SELECT
            day         d1,
            new_cases   n1,
            new_deaths  nd1
        FROM
            project_statistic2020
        WHERE
            country_name = 'India'
    )
    FULL OUTER JOIN (
        SELECT
            day         d2,
            new_cases   n2,
            new_deaths  nd2
        FROM
            project_statistic2020
        WHERE
            country_name = 'United States'
    ) ON d1 = d2
    FULL OUTER JOIN (
        SELECT
            day         d3,
            new_cases   n3,
            new_deaths  nd3
        FROM
            project_statistic2020
        WHERE
            country_name = 'China'
    ) ON d1 = d3
    FULL OUTER JOIN (
        SELECT
            day                  ddbp1,
            diabetes_prevalence  dbp1
        FROM
            project_statisticnew
        WHERE
            country_name = 'India'
    ) ON d1 = ddbp1
    FULL OUTER JOIN (
        SELECT
            day                  ddbp2,
            diabetes_prevalence  dbp2
        FROM
            project_statisticnew
        WHERE
            country_name = 'United States'
    ) ON d1 = ddbp2
    FULL OUTER JOIN (
        SELECT
            day                  ddbp3,
            diabetes_prevalence  dbp3
        FROM
            project_statisticnew
        WHERE
            country_name = 'China'
    ) ON d1 = ddbp3
    FULL OUTER JOIN (
        SELECT
            day                            ds1,
            female_smokers + male_smokers  s1
        FROM
            project_statisticnew
        WHERE
            country_name = 'India'
    ) ON d1 = ds1
    FULL OUTER JOIN (
        SELECT
            day                            ds2,
            female_smokers + male_smokers  s2
        FROM
            project_statisticnew
        WHERE
            country_name = 'United States'
    ) ON d1 = ds2
    FULL OUTER JOIN (
        SELECT
            day                            ds3,
            female_smokers + male_smokers  s3
        FROM
            project_statisticnew
        WHERE
            country_name = 'China'
    ) ON d1 = ds3
    FULL OUTER JOIN (
        SELECT
            day               da1,
            aged_65_or_older  a1
        FROM
            project_statisticnew
        WHERE
            country_name = 'India'
    ) ON d1 = da1
    FULL OUTER JOIN (
        SELECT
            day               da2,
            aged_65_or_older  a2
        FROM
            project_statisticnew
        WHERE
            country_name = 'United States'
    ) ON d1 = da2
    FULL OUTER JOIN (
        SELECT
            day               da3,
            aged_65_or_older  a3
        FROM
            project_statisticnew
        WHERE
            country_name = 'China'
    ) ON d1 = da3
    FULL OUTER JOIN (
        SELECT
            day                     dhw1,
            handwashing_facilities  hw1
        FROM
            project_statisticnew
        WHERE
            country_name = 'India'
    ) ON d1 = dhw1
    FULL OUTER JOIN (
        SELECT
            day                     dhw2,
            handwashing_facilities  hw2
        FROM
            project_statisticnew
        WHERE
            country_name = 'United States'
    ) ON d1 = dhw2
    FULL OUTER JOIN (
        SELECT
            day                     dhw3,
            handwashing_facilities  hw3
        FROM
            project_statisticnew
        WHERE
            country_name = 'China'
    ) ON d1 = dhw3
WHERE
    d1 IS NOT NULL
ORDER BY
    d1;

--QUERY 2 DONE
--Flights and Cases/ Normalized

SELECT
    x1,
    y1,
    y2,
    y3,
    y4
FROM
         (
        SELECT
            m1       AS x1,
            c1 + c2    AS y1
        FROM
                 (
                SELECT
                    EXTRACT(MONTH FROM end_time)     m1,
                    a1.country,
                    COUNT(*)                         c1
                FROM
                         project_flight
                    JOIN project_airport a1 ON departure = a1.icaoap
                WHERE
                    a1.country = 'India'
                GROUP BY
                    EXTRACT(MONTH FROM end_time),
                    a1.country
                ORDER BY
                    EXTRACT(MONTH FROM end_time)
            )
            JOIN (
                SELECT
                    EXTRACT(MONTH FROM end_time)     m2,
                    a2.country,
                    COUNT(*)                         c2
                FROM
                         project_flight
                    JOIN project_airport a2 ON destination = a2.icaoap
                WHERE
                    a2.country = 'India'
                GROUP BY
                    EXTRACT(MONTH FROM end_time),
                    a2.country
                ORDER BY
                    EXTRACT(MONTH FROM end_time)
            ) ON m1 = m2
    )
    NATURAL JOIN (
        SELECT
            EXTRACT(MONTH FROM day)     AS x1,
            SUM(new_cases)              AS y2
        FROM
            project_statistic2020
        WHERE
            country_name = 'India'
        GROUP BY
            EXTRACT(MONTH FROM day)
        ORDER BY
            EXTRACT(MONTH FROM day)
    )
    JOIN (
        SELECT
            m1                         AS x2,
            round(cnt1 + cnt2, 4)        AS y3
        FROM
                 (
                SELECT
                    EXTRACT(MONTH FROM d2)        m2,
                    ( COUNT(*) * 13800 / p2 )           cnt2
                FROM
                    (
                        SELECT
                            end_time          d2,
                            pc2.population    p2,
                            a2.country
                        FROM
                                 project_flight
                            JOIN project_airport  a2 ON destination = a2.icaoap
                            JOIN project_country  pc2 ON a2.country = pc2.name
                        WHERE
                            a2.country = 'India'
                    )
                GROUP BY
                    EXTRACT(MONTH FROM d2),
                    p2
                ORDER BY
                    EXTRACT(MONTH FROM d2)
            )
            JOIN (
                SELECT
                    EXTRACT(MONTH FROM d1)        m1,
                    ( COUNT(*) * 13800 / p1 )           cnt1
                FROM
                    (
                        SELECT
                            end_time          d1,
                            pc1.population    p1,
                            a1.country
                        FROM
                                 project_flight
                            JOIN project_airport  a1 ON departure = a1.icaoap
                            JOIN project_country  pc1 ON a1.country = pc1.name
                        WHERE
                            a1.country = 'India'
                    )
                GROUP BY
                    EXTRACT(MONTH FROM d1),
                    p1
                ORDER BY
                    EXTRACT(MONTH FROM d1)
            ) ON m1 = m2
    ) ON x1 = x2
    NATURAL JOIN (
        SELECT
            EXTRACT(MONTH FROM day)     AS x2,
            round(100 * SUM(new_cases) /(
                SELECT
                    population
                FROM
                    project_country
                WHERE
                    name = 'India'
            ), 8)                       AS y4
        FROM
            project_statistic2020
        WHERE
            country_name = 'India'
        GROUP BY
            EXTRACT(MONTH FROM day)
        ORDER BY
            EXTRACT(MONTH FROM day)
    );

--QUERY 3 DONE
--Combined Hotspot

SELECT
    x,
    nvl(y1, 0)       flights,
    nvl(y2, 0)       cases,
    nvl(y3, 0)       brazil,
    nvl(y4, 0)       china,
    nvl(y5, 0)       india,
    nvl(y6, 0)       unitedstates
FROM
         (
        SELECT
            EXTRACT(MONTH FROM end_time)     AS x,
            COUNT(*)                         AS y1
        FROM
            project_flight
        WHERE
            departure IN (
                SELECT
                    icaoap
                FROM
                    project_airport
                WHERE
                    country IN (
                        SELECT DISTINCT
                            country_name AS hotspots
                        FROM
                            (
                                SELECT
                                    country_name, month, sums,
                                    ROW_NUMBER()
                                    OVER(PARTITION BY month
                                         ORDER BY sums DESC
                                    ) AS rn
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            EXTRACT(MONTH FROM day)     AS month,
                                            SUM(new_cases)              AS sums
                                        FROM
                                            project_statistic2020
                                        WHERE
                                            country_name NOT IN ( 'World', 'International', 'Europe', 'North America', 'Asia',
                                                                  'Africa',
                                                                  'North America',
                                                                  'European Union',
                                                                  'South America' )
                                            AND new_cases IS NOT NULL
                                        GROUP BY
                                            country_name,
                                            EXTRACT(MONTH FROM day)
                                        ORDER BY
                                            month DESC,
                                            SUM(new_cases) DESC
                                    )
                            )
                        WHERE
                            rn <= 1
                    )
            )
            AND destination IN (
                SELECT
                    icaoap
                FROM
                    project_airport
                WHERE
                    country = 'India'
            )
        GROUP BY
            EXTRACT(MONTH FROM end_time)
        ORDER BY
            EXTRACT(MONTH FROM end_time)
    )
    NATURAL JOIN (
        SELECT
            EXTRACT(MONTH FROM day)     AS x,
            SUM(new_cases)              AS y2
        FROM
            project_statistic2020
        WHERE
            country_name = 'India'
        GROUP BY
            EXTRACT(MONTH FROM day)
        ORDER BY
            EXTRACT(MONTH FROM day)
    )
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM end_time)     AS x1,
            COUNT(*)                         AS y3
        FROM
            project_flight
        WHERE
            departure IN (
                SELECT
                    icaoap
                FROM
                    project_airport
                WHERE
                    country IN (
                        SELECT
                            hotspots
                        FROM
                            (
                                SELECT
                                    hotspots, ROW_NUMBER()
                                              OVER(
                                        ORDER BY hotspots
                                              ) rowtobechanged
                                FROM
                                    (
                                        SELECT DISTINCT
                                            country_name AS hotspots
                                        FROM
                                            (
                                                SELECT
                                                    country_name,
                                                    month,
                                                    sums,
                                                    ROW_NUMBER()
                                                    OVER(PARTITION BY month
                                                         ORDER BY sums DESC
                                                    ) AS rn
                                                FROM
                                                    (
                                                        SELECT
                                                            country_name,
                                                            EXTRACT(MONTH FROM day)     AS month,
                                                            SUM(new_cases)              AS sums
                                                        FROM
                                                            project_statistic2020
                                                        WHERE
                                                            country_name NOT IN ( 'World', 'International', 'Europe',
                                                                                  'North America',
                                                                                  'Asia',
                                                                                  'Africa',
                                                                                  'North America',
                                                                                  'European Union',
                                                                                  'South America' )
                                                            AND new_cases IS NOT NULL
                                                        GROUP BY
                                                            country_name,
                                                            EXTRACT(MONTH FROM day)
                                                        ORDER BY
                                                            month DESC,
                                                            SUM(new_cases) DESC
                                                    )
                                            )
                                        WHERE
                                            rn <= 1
                                    )
                            )
                        WHERE
                            rowtobechanged = 1
                    )
            )
            AND destination IN (
                SELECT
                    icaoap
                FROM
                    project_airport
                WHERE
                    country = 'India'
            )
        GROUP BY
            EXTRACT(MONTH FROM end_time)
        ORDER BY
            EXTRACT(MONTH FROM end_time)
    ) ON x = x1
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM end_time)     AS x2,
            COUNT(*)                         AS y4
        FROM
            project_flight
        WHERE
            departure IN (
                SELECT
                    icaoap
                FROM
                    project_airport
                WHERE
                    country IN (
                        SELECT
                            hotspots
                        FROM
                            (
                                SELECT
                                    hotspots, ROW_NUMBER()
                                              OVER(
                                        ORDER BY hotspots
                                              ) rowtobechanged
                                FROM
                                    (
                                        SELECT DISTINCT
                                            country_name AS hotspots
                                        FROM
                                            (
                                                SELECT
                                                    country_name,
                                                    month,
                                                    sums,
                                                    ROW_NUMBER()
                                                    OVER(PARTITION BY month
                                                         ORDER BY sums DESC
                                                    ) AS rn
                                                FROM
                                                    (
                                                        SELECT
                                                            country_name,
                                                            EXTRACT(MONTH FROM day)     AS month,
                                                            SUM(new_cases)              AS sums
                                                        FROM
                                                            project_statistic2020
                                                        WHERE
                                                            country_name NOT IN ( 'World', 'International', 'Europe',
                                                                                  'North America',
                                                                                  'Asia',
                                                                                  'Africa',
                                                                                  'North America',
                                                                                  'European Union',
                                                                                  'South America' )
                                                            AND new_cases IS NOT NULL
                                                        GROUP BY
                                                            country_name,
                                                            EXTRACT(MONTH FROM day)
                                                        ORDER BY
                                                            month DESC,
                                                            SUM(new_cases) DESC
                                                    )
                                            )
                                        WHERE
                                            rn <= 1
                                    )
                            )
                        WHERE
                            rowtobechanged = 2
                    )
            )
            AND destination IN (
                SELECT
                    icaoap
                FROM
                    project_airport
                WHERE
                    country = 'India'
            )
        GROUP BY
            EXTRACT(MONTH FROM end_time)
        ORDER BY
            EXTRACT(MONTH FROM end_time)
    ) ON x2 = x
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM end_time)     AS x3,
            COUNT(*)                         AS y5
        FROM
            project_flight
        WHERE
            departure IN (
                SELECT
                    icaoap
                FROM
                    project_airport
                WHERE
                    country IN (
                        SELECT
                            hotspots
                        FROM
                            (
                                SELECT
                                    hotspots, ROW_NUMBER()
                                              OVER(
                                        ORDER BY hotspots
                                              ) rowtobechanged
                                FROM
                                    (
                                        SELECT DISTINCT
                                            country_name AS hotspots
                                        FROM
                                            (
                                                SELECT
                                                    country_name,
                                                    month,
                                                    sums,
                                                    ROW_NUMBER()
                                                    OVER(PARTITION BY month
                                                         ORDER BY sums DESC
                                                    ) AS rn
                                                FROM
                                                    (
                                                        SELECT
                                                            country_name,
                                                            EXTRACT(MONTH FROM day)     AS month,
                                                            SUM(new_cases)              AS sums
                                                        FROM
                                                            project_statistic2020
                                                        WHERE
                                                            country_name NOT IN ( 'World', 'International', 'Europe',
                                                                                  'North America',
                                                                                  'Asia',
                                                                                  'Africa',
                                                                                  'North America',
                                                                                  'European Union',
                                                                                  'South America' )
                                                            AND new_cases IS NOT NULL
                                                        GROUP BY
                                                            country_name,
                                                            EXTRACT(MONTH FROM day)
                                                        ORDER BY
                                                            month DESC,
                                                            SUM(new_cases) DESC
                                                    )
                                            )
                                        WHERE
                                            rn <= 1
                                    )
                            )
                        WHERE
                            rowtobechanged = 3
                    )
            )
            AND destination IN (
                SELECT
                    icaoap
                FROM
                    project_airport
                WHERE
                    country = 'India'
            )
        GROUP BY
            EXTRACT(MONTH FROM end_time)
        ORDER BY
            EXTRACT(MONTH FROM end_time)
    ) ON x3 = x
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM end_time)     AS x4,
            COUNT(*)                         AS y6
        FROM
            project_flight
        WHERE
            departure IN (
                SELECT
                    icaoap
                FROM
                    project_airport
                WHERE
                    country IN (
                        SELECT
                            hotspots
                        FROM
                            (
                                SELECT
                                    hotspots, ROW_NUMBER()
                                              OVER(
                                        ORDER BY hotspots
                                              ) rowtobechanged
                                FROM
                                    (
                                        SELECT DISTINCT
                                            country_name AS hotspots
                                        FROM
                                            (
                                                SELECT
                                                    country_name,
                                                    month,
                                                    sums,
                                                    ROW_NUMBER()
                                                    OVER(PARTITION BY month
                                                         ORDER BY sums DESC
                                                    ) AS rn
                                                FROM
                                                    (
                                                        SELECT
                                                            country_name,
                                                            EXTRACT(MONTH FROM day)     AS month,
                                                            SUM(new_cases)              AS sums
                                                        FROM
                                                            project_statistic2020
                                                        WHERE
                                                            country_name NOT IN ( 'World', 'International', 'Europe',
                                                                                  'North America',
                                                                                  'Asia',
                                                                                  'Africa',
                                                                                  'North America',
                                                                                  'European Union',
                                                                                  'South America' )
                                                            AND new_cases IS NOT NULL
                                                        GROUP BY
                                                            country_name,
                                                            EXTRACT(MONTH FROM day)
                                                        ORDER BY
                                                            month DESC,
                                                            SUM(new_cases) DESC
                                                    )
                                            )
                                        WHERE
                                            rn <= 1
                                    )
                            )
                        WHERE
                            rowtobechanged = 4
                    )
            )
            AND destination IN (
                SELECT
                    icaoap
                FROM
                    project_airport
                WHERE
                    country = 'India'
            )
        GROUP BY
            EXTRACT(MONTH FROM end_time)
        ORDER BY
            EXTRACT(MONTH FROM end_time)
    ) ON x4 = x;

--QUERY 4 DONE
--Replace "Delta Air Lines" with requested airline

SELECT
    x1,
    y1,
    y2,
    y3
FROM
         (
        SELECT
            x1,
            round(c /(
                SELECT
                    MAX(c)
                FROM
                    (
                        SELECT
                            month        x1, COUNT(*)     c
                        FROM
                            (
                                SELECT
                                    EXTRACT(MONTH FROM end_time)     AS month,
                                    substr(callsign, 1, 3)             AS icao
                                FROM
                                    project_flight
                            )
                        WHERE
                            icao IN(
                                SELECT
                                    icaoal
                                FROM
                                    project_airline
                                WHERE
                                    name = 'Delta Air Lines'
                            )
                        GROUP BY
                            month
                        ORDER BY
                            month
                    )
            ),
                  4) y1
        FROM
            (
                SELECT
                    month        x1,
                    COUNT(*)     c
                FROM
                    (
                        SELECT
                            EXTRACT(MONTH FROM end_time)     AS month,
                            substr(callsign, 1, 3)             AS icao
                        FROM
                            project_flight
                    )
                WHERE
                    icao IN (
                        SELECT
                            icaoal
                        FROM
                            project_airline
                        WHERE
                            name = 'Delta Air Lines'
                    )
                GROUP BY
                    month
                ORDER BY
                    month
            )
    )
    JOIN (
        SELECT
            x2,
            round(c /(
                SELECT
                    MAX(c)
                FROM
                    (
                        SELECT
                            month        x2, COUNT(*)     c
                        FROM
                            (
                                SELECT
                                    EXTRACT(MONTH FROM end_time)     AS month,
                                    substr(callsign, 1, 3)             AS icao
                                FROM
                                    project_flight
                            )
                        WHERE
                            icao IN(
                                SELECT
                                    icaoal
                                FROM
                                    project_airline
                                WHERE
                                    name = 'American Airlines'
                            )
                        GROUP BY
                            month
                        ORDER BY
                            month
                    )
            ),
                  4) y2
        FROM
            (
                SELECT
                    month        x2,
                    COUNT(*)     c
                FROM
                    (
                        SELECT
                            EXTRACT(MONTH FROM end_time)     AS month,
                            substr(callsign, 1, 3)             AS icao
                        FROM
                            project_flight
                    )
                WHERE
                    icao IN (
                        SELECT
                            icaoal
                        FROM
                            project_airline
                        WHERE
                            name = 'American Airlines'
                    )
                GROUP BY
                    month
                ORDER BY
                    month
            )
    ) ON x1 = x2
    JOIN (
        SELECT
            x3,
            round(c /(
                SELECT
                    MAX(c)
                FROM
                    (
                        SELECT
                            month        x3, COUNT(*)     c
                        FROM
                            (
                                SELECT
                                    EXTRACT(MONTH FROM end_time)     AS month,
                                    substr(callsign, 1, 3)             AS icao
                                FROM
                                    project_flight
                            )
                        WHERE
                            icao IN(
                                SELECT
                                    icaoal
                                FROM
                                    project_airline
                                WHERE
                                    name = 'United Airlines'
                            )
                        GROUP BY
                            month
                        ORDER BY
                            month
                    )
            ),
                  4) y3
        FROM
            (
                SELECT
                    month        x3,
                    COUNT(*)     c
                FROM
                    (
                        SELECT
                            EXTRACT(MONTH FROM end_time)     AS month,
                            substr(callsign, 1, 3)             AS icao
                        FROM
                            project_flight
                    )
                WHERE
                    icao IN (
                        SELECT
                            icaoal
                        FROM
                            project_airline
                        WHERE
                            name = 'United Airlines'
                    )
                GROUP BY
                    month
                ORDER BY
                    month
            )
    ) ON x2 = x3;

--QUERY 5

SELECT
    x,
    nvl(c, 0)         c,
    nvl(l1, 0)        l1,
    nvl(l2, 0)        l2,
    nvl(l3, 0)        l3,
    nvl(h1, 0)        h1,
    nvl(h2, 0)        h2,
    nvl(h3, 0)        h3,
    nvl(cs, 0)        cs,
    nvl(l1s, 0)       l1s,
    nvl(l2s, 0)       l2s,
    nvl(l3s, 0)       l3s,
    nvl(h1s, 0)       h1s,
    nvl(h2s, 0)       h2s,
    nvl(h3s, 0)       h3s
FROM
    (
        SELECT
            EXTRACT(MONTH FROM day)                                      x,
            round(SUM((new_deaths) * 1000000 / population), 6)               c
        FROM
                 project_statistic2020
            JOIN project_country ON country_name = name
        WHERE
            country_name = 'India'
        GROUP BY
            EXTRACT(MONTH FROM day),
            population
        ORDER BY
            EXTRACT(MONTH FROM day)
    )
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                                      xl1,
            round(SUM((new_deaths) * 1000000 / population), 6)                l1
        FROM
                 project_statistic2020
            JOIN project_country ON country_name = name
        WHERE
            country_name = (
                SELECT
                    leaststrin
                FROM
                    (
                        SELECT
                            country_name  leaststrin,
                            ROW_NUMBER()
                            OVER(
                                ORDER BY s
                            )             rn
                        FROM
                            (
                                SELECT
                                    country_name,
                                    round(AVG(stringency_index), 4) s
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            stringency_index
                                        FROM
                                            project_statisticnew
                                        WHERE
                                            stringency_index IS NOT NULL
                                    )
                                GROUP BY
                                    country_name
                                ORDER BY
                                    s
                            )
                        WHERE
                            ROWNUM < 4
                    )
                WHERE
                    rn = 1
            )
        GROUP BY
            EXTRACT(MONTH FROM day),
            population
        ORDER BY
            EXTRACT(MONTH FROM day)
    ) ON x = xl1
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                                      xl2,
            round(SUM((new_deaths) * 1000000 / population), 6)                l2
        FROM
                 project_statistic2020
            JOIN project_country ON country_name = name
        WHERE
            country_name = (
                SELECT
                    leaststrin
                FROM
                    (
                        SELECT
                            country_name  leaststrin,
                            ROW_NUMBER()
                            OVER(
                                ORDER BY s
                            )             rn
                        FROM
                            (
                                SELECT
                                    country_name,
                                    round(AVG(stringency_index), 4) s
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            stringency_index
                                        FROM
                                            project_statisticnew
                                        WHERE
                                            stringency_index IS NOT NULL
                                    )
                                GROUP BY
                                    country_name
                                ORDER BY
                                    s
                            )
                        WHERE
                            ROWNUM < 4
                    )
                WHERE
                    rn = 2
            )
        GROUP BY
            EXTRACT(MONTH FROM day),
            population
        ORDER BY
            EXTRACT(MONTH FROM day)
    ) ON xl2 = x
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                                      xl3,
            round(SUM((new_deaths) * 1000000 / population), 6)                l3
        FROM
                 project_statistic2020
            JOIN project_country ON country_name = name
        WHERE
            country_name = (
                SELECT
                    leaststrin
                FROM
                    (
                        SELECT
                            country_name  leaststrin,
                            ROW_NUMBER()
                            OVER(
                                ORDER BY s
                            )             rn
                        FROM
                            (
                                SELECT
                                    country_name,
                                    round(AVG(stringency_index), 4) s
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            stringency_index
                                        FROM
                                            project_statisticnew
                                        WHERE
                                            stringency_index IS NOT NULL
                                    )
                                GROUP BY
                                    country_name
                                ORDER BY
                                    s
                            )
                        WHERE
                            ROWNUM < 4
                    )
                WHERE
                    rn = 3
            )
        GROUP BY
            EXTRACT(MONTH FROM day),
            population
        ORDER BY
            EXTRACT(MONTH FROM day)
    ) ON x = xl3
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                                      xh1,
            round(SUM((new_deaths) * 1000000 / population), 6)                h1
        FROM
                 project_statistic2020
            JOIN project_country ON country_name = name
        WHERE
            country_name = (
                SELECT
                    moststrin
                FROM
                    (
                        SELECT
                            country_name  moststrin,
                            ROW_NUMBER()
                            OVER(
                                ORDER BY s
                            )             rn
                        FROM
                            (
                                SELECT
                                    country_name,
                                    round(AVG(stringency_index), 4) s
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            stringency_index
                                        FROM
                                            project_statisticnew
                                        WHERE
                                            stringency_index IS NOT NULL
                                    )
                                GROUP BY
                                    country_name
                                ORDER BY
                                    s DESC
                            )
                        WHERE
                            ROWNUM < 4
                    )
                WHERE
                    rn = 1
            )
        GROUP BY
            EXTRACT(MONTH FROM day),
            population
        ORDER BY
            EXTRACT(MONTH FROM day)
    ) ON x = xh1
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                                      xh2,
            round(SUM((new_deaths) * 1000000 / population), 6)                h2
        FROM
                 project_statistic2020
            JOIN project_country ON country_name = name
        WHERE
            country_name = (
                SELECT
                    moststrin
                FROM
                    (
                        SELECT
                            country_name  moststrin,
                            ROW_NUMBER()
                            OVER(
                                ORDER BY s
                            )             rn
                        FROM
                            (
                                SELECT
                                    country_name,
                                    round(AVG(stringency_index), 4) s
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            stringency_index
                                        FROM
                                            project_statisticnew
                                        WHERE
                                            stringency_index IS NOT NULL
                                    )
                                GROUP BY
                                    country_name
                                ORDER BY
                                    s DESC
                            )
                        WHERE
                            ROWNUM < 4
                    )
                WHERE
                    rn = 2
            )
        GROUP BY
            EXTRACT(MONTH FROM day),
            population
        ORDER BY
            EXTRACT(MONTH FROM day)
    ) ON x = xh2
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                                      xh3,
            round(SUM((new_deaths) * 1000000 / population), 6)                h3
        FROM
                 project_statistic2020
            JOIN project_country ON country_name = name
        WHERE
            country_name = (
                SELECT
                    moststrin
                FROM
                    (
                        SELECT
                            country_name  moststrin,
                            ROW_NUMBER()
                            OVER(
                                ORDER BY s
                            )             rn
                        FROM
                            (
                                SELECT
                                    country_name,
                                    round(AVG(stringency_index), 4) s
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            stringency_index
                                        FROM
                                            project_statisticnew
                                        WHERE
                                            stringency_index IS NOT NULL
                                    )
                                GROUP BY
                                    country_name
                                ORDER BY
                                    s DESC
                            )
                        WHERE
                            ROWNUM < 4
                    )
                WHERE
                    rn = 3
            )
        GROUP BY
            EXTRACT(MONTH FROM day),
            population
        ORDER BY
            EXTRACT(MONTH FROM day)
    ) ON x = xh3
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                 xl1s,
            round(AVG(stringency_index), 6)         l1s
        FROM
            project_statisticnew
        WHERE
            country_name = (
                SELECT
                    leaststrin
                FROM
                    (
                        SELECT
                            country_name  leaststrin,
                            ROW_NUMBER()
                            OVER(
                                ORDER BY s
                            )             rn
                        FROM
                            (
                                SELECT
                                    country_name,
                                    round(AVG(stringency_index), 4) s
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            stringency_index
                                        FROM
                                            project_statisticnew
                                        WHERE
                                            stringency_index IS NOT NULL
                                    )
                                GROUP BY
                                    country_name
                                ORDER BY
                                    s
                            )
                        WHERE
                            ROWNUM < 4
                    )
                WHERE
                    rn = 1
            )
        GROUP BY
            EXTRACT(MONTH FROM day)
        ORDER BY
            xl1s
    ) ON xl1s = x
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                 xl2s,
            round(AVG(stringency_index), 6)         l2s
        FROM
            project_statisticnew
        WHERE
            country_name = (
                SELECT
                    leaststrin
                FROM
                    (
                        SELECT
                            country_name  leaststrin,
                            ROW_NUMBER()
                            OVER(
                                ORDER BY s
                            )             rn
                        FROM
                            (
                                SELECT
                                    country_name,
                                    round(AVG(stringency_index), 4) s
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            stringency_index
                                        FROM
                                            project_statisticnew
                                        WHERE
                                            stringency_index IS NOT NULL
                                    )
                                GROUP BY
                                    country_name
                                ORDER BY
                                    s
                            )
                        WHERE
                            ROWNUM < 4
                    )
                WHERE
                    rn = 2
            )
        GROUP BY
            EXTRACT(MONTH FROM day)
        ORDER BY
            xl2s
    ) ON xl2s = x
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                 xl3s,
            round(AVG(stringency_index), 6)         l3s
        FROM
            project_statisticnew
        WHERE
            country_name = (
                SELECT
                    leaststrin
                FROM
                    (
                        SELECT
                            country_name  leaststrin,
                            ROW_NUMBER()
                            OVER(
                                ORDER BY s
                            )             rn
                        FROM
                            (
                                SELECT
                                    country_name,
                                    round(AVG(stringency_index), 4) s
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            stringency_index
                                        FROM
                                            project_statisticnew
                                        WHERE
                                            stringency_index IS NOT NULL
                                    )
                                GROUP BY
                                    country_name
                                ORDER BY
                                    s
                            )
                        WHERE
                            ROWNUM < 4
                    )
                WHERE
                    rn = 3
            )
        GROUP BY
            EXTRACT(MONTH FROM day)
        ORDER BY
            xl3s
    ) ON xl3s = x
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                 xh1s,
            round(AVG(stringency_index), 6)         h1s
        FROM
            project_statisticnew
        WHERE
            country_name = (
                SELECT
                    leaststrin
                FROM
                    (
                        SELECT
                            country_name  leaststrin,
                            ROW_NUMBER()
                            OVER(
                                ORDER BY s
                            )             rn
                        FROM
                            (
                                SELECT
                                    country_name,
                                    round(AVG(stringency_index), 4) s
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            stringency_index
                                        FROM
                                            project_statisticnew
                                        WHERE
                                            stringency_index IS NOT NULL
                                    )
                                GROUP BY
                                    country_name
                                ORDER BY
                                    s DESC
                            )
                        WHERE
                            ROWNUM < 4
                    )
                WHERE
                    rn = 1
            )
        GROUP BY
            EXTRACT(MONTH FROM day)
        ORDER BY
            xh1s
    ) ON xh1s = x
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                 xh2s,
            round(AVG(stringency_index), 6)         h2s
        FROM
            project_statisticnew
        WHERE
            country_name = (
                SELECT
                    leaststrin
                FROM
                    (
                        SELECT
                            country_name  leaststrin,
                            ROW_NUMBER()
                            OVER(
                                ORDER BY s
                            )             rn
                        FROM
                            (
                                SELECT
                                    country_name,
                                    round(AVG(stringency_index), 4) s
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            stringency_index
                                        FROM
                                            project_statisticnew
                                        WHERE
                                            stringency_index IS NOT NULL
                                    )
                                GROUP BY
                                    country_name
                                ORDER BY
                                    s DESC
                            )
                        WHERE
                            ROWNUM < 4
                    )
                WHERE
                    rn = 2
            )
        GROUP BY
            EXTRACT(MONTH FROM day)
        ORDER BY
            xh2s
    ) ON xh2s = x
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                 xh3s,
            round(AVG(stringency_index), 6)         h3s
        FROM
            project_statisticnew
        WHERE
            country_name = (
                SELECT
                    leaststrin
                FROM
                    (
                        SELECT
                            country_name  leaststrin,
                            ROW_NUMBER()
                            OVER(
                                ORDER BY s
                            )             rn
                        FROM
                            (
                                SELECT
                                    country_name,
                                    round(AVG(stringency_index), 4) s
                                FROM
                                    (
                                        SELECT
                                            country_name,
                                            stringency_index
                                        FROM
                                            project_statisticnew
                                        WHERE
                                            stringency_index IS NOT NULL
                                    )
                                GROUP BY
                                    country_name
                                ORDER BY
                                    s DESC
                            )
                        WHERE
                            ROWNUM < 4
                    )
                WHERE
                    rn = 3
            )
        GROUP BY
            EXTRACT(MONTH FROM day)
        ORDER BY
            xh3s
    ) ON xh3s = x
    FULL OUTER JOIN (
        SELECT
            EXTRACT(MONTH FROM day)                 xcs,
            round(AVG(stringency_index), 6)         cs
        FROM
            project_statisticnew
        WHERE
            country_name = 'India'
        GROUP BY
            EXTRACT(MONTH FROM day)
        ORDER BY
            xcs
    ) ON xcs = x
ORDER BY
    x;