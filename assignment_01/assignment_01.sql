SELECT *
FROM lithan.SPPEProcessAudit
WHERE
    field2 = '1413'
AND
	spPEProcessStateId IN
(SELECT spPEProcessStateId
FROM
	lithan.SPPEProcessState
WHERE
	createDate >= '2021-01-01 00:00:00'
)
;