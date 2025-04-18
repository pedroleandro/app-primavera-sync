SELECT NUMPED
FROM PCPEDC
INNER JOIN PCCLIENT ON PCCLIENT.CODCLI = PCPEDC.CODCLI
INNER JOIN PCFILIAL ON PCFILIAL.CODIGO = PCPEDC.CODFILIAL
WHERE PCPEDC.DATA >= TO_DATE('01/01/2025', 'DD/MM/YYYY')
AND PCPEDC.CODFILIAL IN ('31', '52')
AND PCPEDC.POSICAO = 'C'
ORDER BY PCPEDC.DATA