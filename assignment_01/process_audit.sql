SELECT *
FROM lithan.SPPEProcessAudit
WHERE
	(
		data1 LIKE '%CMFBLGSGSGUDMREQ222%'
	OR
		data1 LIKE '%CMFBLGSGSGUDSREQ222%'
    )
AND
	spPEProcessStateId IN
(SELECT spPEProcessStateId
FROM
	lithan.SPPEProcessState
WHERE
	createDate BETWEEN
    '2021-10-01 00:00:00'
AND
	'2021-10-15 23:59:59'
)
;