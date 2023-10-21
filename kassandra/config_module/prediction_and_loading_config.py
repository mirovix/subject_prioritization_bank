#!/usr/bin/env python3

"""
@Author: Miro
@Date: 14/06/2023
@Version: 1.0
@Objective: configuration for prediction_and_loading package
@TODO: rapport nan values
"""

ndg_name = 'NDG'

name_subject_col = 'NAME'
office_subject_col = 'TARGET_OFFICE'
fiscal_code_subject_col = 'FISCAL_CODE'
residence_country_subject_col = 'RESIDENCE_COUNTRY'
sae_subject_col = 'SAE'
ateco_subject_col = 'ATECO'
residence_city_subject_col = 'RESIDENCE_CITY'
juridical_nature_subject_col = 'LEGAL_SPECIE'
birth_date_subject_col = 'BIRTH_DAY'

name_attribute_xml = 'Nominativo'
office_attribute_xml = 'Office'
fiscal_code_attribute_xml = 'Codice Fiscale'
juridical_nature_attribute_xml = 'Natura Giuridica'
birth_date_attribute_xml = 'Data di Nascita'
residence_city_attribute_xml = 'Città di Residenza'
residence_country_attribute_xml = 'Paese di residenza'
sae_attribute_xml = 'SAE'
ateco_attribute_xml = 'ATECO'
ref_month_xml = 'Mese di Riferimento'
prediction_score_xml = 'K Score'

table_name = 'Features'
table_header_description = "DESCRIZIONE"
table_header_value_name = "VALORE"
table_header_contribution = "CONTRIBUTO"

ndg_length = 16
residence_country_length = 3

round_percentage = 2
round_contributions = 5
round_prediction_score = 4


boolean_features_values = {0: 'No', 1: 'Si'}
boolean_features_name = ['NDG', 'STATUS', 'AGE_0_9', 'AGE_10-19', 'AGE_20-29', 'AGE_30-39',
                         'AGE_40-49', 'AGE_50-59', 'AGE_60-69', 'AGE_70-79', 'AGE_80-89',
                         'AGE_90-99', 'AGE_NOT_FOUND', 'LEGAL_SPECIE_DI', 'LEGAL_SPECIE_PF',
                         'LEGAL_SPECIE_PG', 'RESIDENCE_PROVINCE_PRV_NONE',
                         'RESIDENCE_PROVINCE_PRV_0', 'RESIDENCE_PROVINCE_PRV_1',
                         'RESIDENCE_PROVINCE_PRV_2', 'RESIDENCE_PROVINCE_PRV_3',
                         'RESIDENCE_PROVINCE_PRV_4', 'RESIDENCE_PROVINCE_PRV_OTHER', 'SAE_SAE_NONE',
                         'SAE_SAE_0', 'SAE_SAE_1', 'SAE_SAE_2', 'SAE_SAE_3', 'SAE_SAE_4',
                         'SAE_SAE_OTHER', 'ATECO_ATECO_0', 'ATECO_ATECO_1', 'ATECO_ATECO_2',
                         'ATECO_ATECO_3', 'ATECO_ATECO_OTHER', 'ATECO_ATECO_NONE', 'REGIONE_ABRUZZO',
                         'REGIONE_BASILICATA', 'REGIONE_CALABRIA', 'REGIONE_CAMPANIA',
                         'REGIONE_EMILIA-ROMAGNA', 'REGIONE_FRIULI-VENEZIA GIULIA',
                         'REGIONE_LAZIO', 'REGIONE_LIGURIA', 'REGIONE_LOMBARDIA',
                         'REGIONE_MARCHE', 'REGIONE_MOLISE', 'REGIONE_PIEMONTE',
                         'REGIONE_PUGLIA', 'REGIONE_SARDEGNA', 'REGIONE_SICILIA',
                         'REGIONE_TOSCANA', 'REGIONE_TRENTINO-ALTO ADIGE/SÜDTIROL',
                         'REGIONE_UMBRIA', "REGIONE_VALLE D'AOSTA/VALLÉE D'AOSTE",
                         'REGIONE_VENETO', 'REGIONE_OTHER', 'ZONA GEOGRAFICA_CENTRO', 'ZONA GEOGRAFICA_ISOLE',
                         'ZONA GEOGRAFICA_NORD-EST', 'ZONA GEOGRAFICA_NORD-OVEST',
                         'ZONA GEOGRAFICA_SUD', 'PROVINCIA DI CONFINE_NO', 'ZONA GEOGRAFICA_OTHER',
                         'PROVINCIA DI CONFINE_SI', 'PROVINCIA TURISTICA_NO', 'PROVINCIA DI CONFINE_OTHER',
                         'PROVINCIA TURISTICA_SI', 'PROVINCIA TURISTICA_OTHER', 'OCCUPAZIONE TOTALE RANGED_ALTO',
                         'OCCUPAZIONE TOTALE RANGED_BASSO', 'OCCUPAZIONE TOTALE RANGED_MEDIO',
                         'OCCUPAZIONE TOTALE RANGED_OTHER', 'OCCUPAZIONE TOTALI RANGED_ALTO',
                         '% GRADO DI EVASIONE RANGED_ALTO', '% GRADO DI EVASIONE RANGED_BASSO',
                         '% GRADO DI EVASIONE RANGED_MEDIO', '% GRADO DI EVASIONE RANGED_OTHER',
                         'RAPPORTO IMPRESE PER 100 ABITANTI RANGED_ALTO',
                         'RAPPORTO IMPRESE PER 100 ABITANTI RANGED_BASSO',
                         'RAPPORTO IMPRESE PER 100 ABITANTI RANGED_MEDIO',
                         'RAPPORTO IMPRESE PER 100 ABITANTI RANGED_OTHER',
                         'RAPPORTO SPORTELLI BANCARI OGNI 10000 ABITANTI RANGED_ALTO',
                         'RAPPORTO SPORTELLI BANCARI OGNI 10000 ABITANTI RANGED_BASSO',
                         'RAPPORTO SPORTELLI BANCARI OGNI 10000 ABITANTI RANGED_MEDIO',
                         'RAPPORTO SPORTELLI BANCARI OGNI 10000 ABITANTI RANGED_OTHER',
                         'DENUNCE RICICLAGGIO RANGED_ALTO', 'DENUNCE RICICLAGGIO RANGED_BASSO',
                         'DENUNCE RICICLAGGIO RANGED_MEDIO', 'DENUNCE RICICLAGGIO RANGED_OTHER',
                         'ASSOCIAZIONE DI TIPO MAFIOSO RANGED_ALTO', 'ASSOCIAZIONE DI TIPO MAFIOSO RANGED_BASSO',
                         'ASSOCIAZIONE DI TIPO MAFIOSO RANGED_MEDIO', 'ASSOCIAZIONE DI TIPO MAFIOSO RANGED_OTHER']

to_round_features = ['AVG_FREQ_A', 'AVG_FREQ_D',
                   'AVG_COD_CONTANTE_FREQ_A', 'AVG_COD_CONTANTE_FREQ_D',
                   'AVG_COD_ASSEGNI_FREQ_A', 'AVG_COD_ASSEGNI_FREQ_D',
                   'AVG_COD_FINANZIAMENTI_FREQ_A', 'AVG_COD_FINANZIAMENTI_FREQ_D',
                   'AVG_COD_BONIFICI_DOMESTICI_FREQ_A', 'AVG_COD_BONIFICI_DOMESTICI_FREQ_D',
                   'AVG_COD_BONIFICI_ESTERI_FREQ_A', 'AVG_COD_BONIFICI_ESTERI_FREQ_D',
                   'AVG_COD_TIT_CER_INV_FREQ_A', 'AVG_COD_TIT_CER_INV_FREQ_D',
                   'AVG_COD_POS_FREQ_A', 'AVG_COD_POS_FREQ_D',
                   'AVG_COD_PAG_INC_DIVERSI_FREQ_A', 'AVG_COD_PAG_INC_DIVERSI_FREQ_D',
                   'AVG_COD_EFF_DOC_RIBA_FREQ_A', 'AVG_COD_EFF_DOC_RIBA_FREQ_D',
                   'AVG_COD_DIVIDENDI_FREQ_A', 'AVG_COD_DIVIDENDI_FREQ_D',
                   'AVG_COD_REVERSALI_FREQ_A', 'AVG_COD_REVERSALI_FREQ_D',
                   'AVG_RISCHIO_PAESE_A', 'AVG_RISCHIO_PAESE_D']

numerical_features_name = ['NOT_TO_ALERT_FREQ', 'RISK_PROFILE', 'NCHECKREQUIRED', 'NCHECKDEBITED', 'NCHECKAVAILABLE']

risk_profile_name = ['RISK_PROFILE']
risk_profile_values = {1: 'Basso', 2: 'Medio', 3: 'Alto'}

currency_features_name = ['AVG_AMOUNT_A', 'AVG_AMOUNT_D', 'RISCHIO_PAESE_TOT_A', 'RISCHIO_PAESE_TOT_D', 'GROSS_INCOME']
currency_features_values = "€"

""" xml_configuration """
features_dict = {'AVG_FREQ_A': 'Frequenza media mensile di Operazioni in Ingresso',
                 'AVG_FREQ_D': 'Frequenza media mensile di Operazioni in Uscita',
                 'AVG_AMOUNT_A': "Media mensile dell'importo totale in Ingresso",
                 'AVG_AMOUNT_D': "Media mensile dell'importo totale in Uscita",
                 'RISCHIO_PAESE_TOT_A': 'Importo Totale da paesi a Rischio in Ingresso',
                 'RISCHIO_PAESE_TOT_D': 'Importo Totale da paesi a Rischio in Uscita',
                 'AVG_COD_CONTANTE_FREQ_A': 'Frequenza media mensile di Operazioni in Contanti in Ingresso',
                 'AVG_COD_CONTANTE_FREQ_D': 'Frequenza media mensile di Operazioni in Contanti in Uscita',
                 'AVG_COD_ASSEGNI_FREQ_A': 'Frequenza media mensile di Operazioni con Assegni in Ingresso',
                 'AVG_COD_ASSEGNI_FREQ_D': 'Frequenza media mensile di Operazioni con Assegni in Uscita',
                 'AVG_COD_FINANZIAMENTI_FREQ_A': 'Frequenza media mensile di Operazioni relative a Finanziamenti in Ingresso',
                 'AVG_COD_FINANZIAMENTI_FREQ_D': 'Frequenza media mensile di Operazioni relative a Finanziamenti in Uscita',
                 'AVG_COD_BONIFICI_DOMESTICI_FREQ_A': 'Frequenza media mensile di Bonifici Domestici in Ingresso',
                 'AVG_COD_BONIFICI_DOMESTICI_FREQ_D': 'Frequenza media mensile di Bonifici Domestici in Uscita',
                 'AVG_COD_BONIFICI_ESTERI_FREQ_A': 'Frequenza media mensile di Bonifici Esteri in Ingresso',
                 'AVG_COD_BONIFICI_ESTERI_FREQ_D': 'Frequenza media mensile di Bonifici Esteri in Uscita',
                 'AVG_COD_TIT_CER_INV_FREQ_A': 'Frequenza media mensile di Operatività Titoli, Certificati di Deposito e Investimenti in Ingresso',
                 'AVG_COD_TIT_CER_INV_FREQ_D': 'Frequenza media mensile di Operatività Titoli, Certificati di Deposito e Investimenti in Uscita',
                 'AVG_COD_POS_FREQ_A': 'Frequenza media mensile di Operazioni con POS in Ingresso',
                 'AVG_COD_POS_FREQ_D': 'Frequenza media mensile di Operazioni con POS in Uscita',
                 'AVG_COD_PAG_INC_DIVERSI_FREQ_A': 'Frequenza media mensile di Incassi Diversi in Ingresso',
                 'AVG_COD_PAG_INC_DIVERSI_FREQ_D': 'Frequenza media mensile di Incassi Diversi in Uscita',
                 'AVG_COD_EFF_DOC_RIBA_FREQ_A': 'Frequenza media mensile di Effetti, Crediti Documentali e RIBA in Ingresso',
                 'AVG_COD_EFF_DOC_RIBA_FREQ_D': 'Frequenza media mensile di Effetti, Crediti Documentali e RIBA in Uscita',
                 'AVG_COD_DIVIDENDI_FREQ_A': 'Frequenza media mensile di Dividendi in Ingresso',
                 'AVG_COD_DIVIDENDI_FREQ_D': 'Frequenza media mensile di Dividendi in Uscita',
                 'AVG_COD_REVERSALI_FREQ_A': 'Frequenza media mensile di Reversali in Ingresso',
                 'AVG_COD_REVERSALI_FREQ_D': 'Frequenza media mensile di Reversali in Uscita',
                 'AVG_RISCHIO_PAESE_A': "Media mensile dell'importo totale in Ingresso da Paesi a Rischio",
                 'AVG_RISCHIO_PAESE_D': "Media mensile dell'importo totale in Uscita da Paesi a Rischio",
                 'NOT_TO_ALERT_FREQ': "Numero di Anomalie indicate come 'Da non segnalare' negli ultimi 12 mesi",
                 'GROSS_INCOME': "Retribuzione Annua Lorda RAL'",
                 'RISK_PROFILE': 'Profilo di rischio',
                 'NCHECKREQUIRED': 'Numero assegni richiesti',
                 'NCHECKDEBITED': 'Numero assegni utilizzati dal cliente',
                 'NCHECKAVAILABLE': 'Numero assegni a mano cliente',
                 'AGE_0-9': 'Età tra 0 e 10 anni',
                 'AGE_10-19': 'Età tra 10 e 20 anni',
                 'AGE_20-29': 'Età tra 20 e 30 anni',
                 'AGE_30-39': 'Età tra 30 e 40 anni',
                 'AGE_40-49': 'Età tra 40 e 50 anni',
                 'AGE_50-59': 'Età tra 50 e 60 anni',
                 'AGE_60-69': 'Età tra 60 e 70 anni',
                 'AGE_70-79': 'Età tra 70 e 80 anni',
                 'AGE_80-89': 'Età tra 80 e 90 anni',
                 'AGE_90-99': 'Età tra 90 e 100 anni',
                 'AGE_NOT_FOUND': 'Età non Disponibile',
                 'LEGAL_SPECIE_DI': 'Ditta Individuale',
                 'LEGAL_SPECIE_PF': 'Persona Fisica',
                 'LEGAL_SPECIE_PG': 'Persona Giuridica',
                 'RESIDENCE_PROVINCE_PRV_NONE': 'Provincia di Residenza Non disponibile',
                 'RESIDENCE_PROVINCE_PRV_0': 'Provincia di Residenza a rischio irrilevante',
                 'RESIDENCE_PROVINCE_PRV_1': 'Provincia di Residenza a rischio basso',
                 'RESIDENCE_PROVINCE_PRV_2': 'Provincia di Residenza a rischio medio',
                 'RESIDENCE_PROVINCE_PRV_3': 'Provincia di Residenza a rischio elevato',
                 'RESIDENCE_PROVINCE_PRV_4': 'Provincia di Residenza a rischio molto elevato',
                 'RESIDENCE_PROVINCE_PRV_OTHER': 'Provincia di Residenza (altro)',
                 'SAE_SAE_NONE': 'SAE – Settore Attività Economica non disponibile',
                 'SAE_SAE_0': 'SAE – Settore Attività Economica a rischio irrilevante',
                 'SAE_SAE_1': 'SAE – Settore Attività Economica a rischio basso',
                 'SAE_SAE_2': 'SAE – Settore Attività Economica a rischio medio',
                 'SAE_SAE_3': 'SAE – Settore Attività Economica a rischio elevato',
                 'SAE_SAE_4': 'SAE – Settore Attività Economica a rischio molto elevato',
                 'SAE_SAE_OTHER': 'SAE – Settore Attività Economica (altro)',
                 'ATECO_ATECO_0': 'ATECO – Attività Economica a rischio irrilevante',
                 'ATECO_ATECO_1': 'ATECO – Attività Economica a rischio basso',
                 'ATECO_ATECO_2': 'ATECO – Attività Economica a rischio medio',
                 'ATECO_ATECO_3': 'ATECO – Attività Economica a rischio elevato',
                 'ATECO_ATECO_OTHER': 'ATECO – Attività Economica (altro)',
                 'ATECO_ATECO_NONE': 'ATECO – Attività Economica non disponibile',
                 'REGIONE_ABRUZZO': 'Residenza: Abruzzo',
                 'REGIONE_BASILICATA': 'Residenza: Basilicata',
                 'REGIONE_CALABRIA': 'Residenza: Calabria',
                 'REGIONE_CAMPANIA': 'Residenza: Campania',
                 'REGIONE_EMILIA-ROMAGNA': 'Residenza: Emilia-Romagna',
                 'REGIONE_FRIULI-VENEZIA GIULIA': 'Residenza: Friuli-Venezia Giulia',
                 'REGIONE_LAZIO': 'Residenza: Lazio',
                 'REGIONE_LIGURIA': 'Residenza: Liguria',
                 'REGIONE_LOMBARDIA': 'Residenza: Lombardia',
                 'REGIONE_MARCHE': 'Residenza: Marche',
                 'REGIONE_MOLISE': 'Residenza: Molise',
                 'REGIONE_PIEMONTE': 'Residenza: Piemonte',
                 'REGIONE_PUGLIA': 'Residenza: Puglia',
                 'REGIONE_SARDEGNA': 'Residenza: Sardegna',
                 'REGIONE_SICILIA': 'Residenza: Sicilia',
                 'REGIONE_TOSCANA': 'Residenza: Toscana',
                 'REGIONE_TRENTINO-ALTO ADIGE/SÜDTIROL': 'Residenza: Trentino-Alto Adige/Südtirol',
                 'REGIONE_UMBRIA': 'Residenza: Umbria',
                 "REGIONE_VALLE D'AOSTA/VALLÉE D'AOSTE": "Residenza: Valle d'Aosta/Vallée d'Aoste",
                 'REGIONE_VENETO': 'Residenza: Veneto',
                 'REGIONE_OTHER': 'Residenza: Altro',
                 'ZONA GEOGRAFICA_CENTRO': 'Zona Geografica di Residenza: Centro',
                 'ZONA GEOGRAFICA_ISOLE': 'Zona Geografica di Residenza: Isole',
                 'ZONA GEOGRAFICA_NORD-EST': 'Zona Geografica di Residenza: Nord-est',
                 'ZONA GEOGRAFICA_NORD-OVEST': 'Zona Geografica di Residenza: Nord-ovest',
                 'ZONA GEOGRAFICA_SUD': 'Zona Geografica di Residenza: Sud',
                 'ZONA GEOGRAFICA_OTHER': 'Zona Geografica di Residenza: Altro',
                 'PROVINCIA DI CONFINE_NO': 'Provincia di Residenza NON di Confine',
                 'PROVINCIA DI CONFINE_SI': 'Provincia di Residenza di Confine',
                 'PROVINCIA DI CONFINE_OTHER': 'Provincia di Residenza di Confine (altro)',
                 'PROVINCIA TURISTICA_NO': 'Provincia di Residenza NON Turistica',
                 'PROVINCIA TURISTICA_SI': 'Provincia di Residenza Turistica',
                 'PROVINCIA TURISTICA_OTHER': 'Provincia di Residenza Turistica (altro)',
                 'OCCUPAZIONE TOTALE RANGED_ALTO': 'Provincia ad elevato grado di occupazione',
                 'OCCUPAZIONE TOTALE RANGED_BASSO': 'Provincia a basso grado di occupazione',
                 'OCCUPAZIONE TOTALE RANGED_MEDIO': 'Provincia a media grado di occupazione',
                 'OCCUPAZIONE TOTALE RANGED_OTHER': 'Provincia a grado di occupazione (altro)',
                 '% GRADO DI EVASIONE RANGED_ALTO': 'Provincia ad elevato grado di Evasione Fiscale',
                 '% GRADO DI EVASIONE RANGED_BASSO': 'Provincia a basso grado di Evasione Fiscale',
                 '% GRADO DI EVASIONE RANGED_MEDIO': 'Provincia a media grado di Evasione Fiscale',
                 '% GRADO DI EVASIONE RANGED_OTHER': 'Provincia a grado di Evasione Fiscale (altro)',
                 'RAPPORTO IMPRESE PER 100 ABITANTI RANGED_ALTO': 'Provincia ad elevata densità di imprese per numero di abitanti',
                 'RAPPORTO IMPRESE PER 100 ABITANTI RANGED_BASSO': 'Provincia a basso densità di imprese per numero di abitanti',
                 'RAPPORTO IMPRESE PER 100 ABITANTI RANGED_MEDIO': 'Provincia a media densità di imprese per numero di abitanti',
                 'RAPPORTO IMPRESE PER 100 ABITANTI RANGED_OTHER': 'Provincia a densità di imprese per numero di abitanti (altro)',
                 'RAPPORTO SPORTELLI BANCARI OGNI 10000 ABITANTI RANGED_ALTO': 'Provincia ad elevata densità di Sportelli Bancari per numero di abitanti',
                 'RAPPORTO SPORTELLI BANCARI OGNI 10000 ABITANTI RANGED_BASSO': 'Provincia a basso densità di Sportelli Bancari per numero di abitanti',
                 'RAPPORTO SPORTELLI BANCARI OGNI 10000 ABITANTI RANGED_MEDIO': 'Provincia a media densità di Sportelli Bancari per numero di abitanti',
                 'RAPPORTO SPORTELLI BANCARI OGNI 10000 ABITANTI RANGED_OTHER': 'Provincia a densità di Sportelli Bancari per numero di abitanti (altro)',
                 'DENUNCE RICICLAGGIO RANGED_ALTO': 'Provincia ad elevata numerosità di Denunce per Riciclaggio',
                 'DENUNCE RICICLAGGIO RANGED_BASSO': 'Provincia a basso numerosità di Denunce per Riciclaggio',
                 'DENUNCE RICICLAGGIO RANGED_MEDIO': 'Provincia a media numerosità di Denunce per Riciclaggio',
                 'DENUNCE RICICLAGGIO RANGED_OTHER': 'Provincia a numerosità di Denunce per Riciclaggio (altro)',
                 'ASSOCIAZIONE DI TIPO MAFIOSO RANGED_ALTO': 'Provincia ad elevata numerosità di Denunce per Associazione di Tipo Mafioso',
                 'ASSOCIAZIONE DI TIPO MAFIOSO RANGED_BASSO': 'Provincia a basso numerosità di Denunce per Associazione di Tipo Mafioso',
                 'ASSOCIAZIONE DI TIPO MAFIOSO RANGED_MEDIO': 'Provincia a media numerosità di Denunce per Associazione di Tipo Mafioso',
                 'ASSOCIAZIONE DI TIPO MAFIOSO RANGED_OTHER': 'Provincia a numerosità di Denunce per Associazione di Tipo Mafioso (altro)'}