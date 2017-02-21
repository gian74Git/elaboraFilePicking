from configparser import ConfigParser
import pymssql
import imaplib
import email
import datetime
import struct
# import logging
import os
import sys
# from win32com.shell import shell, shellcon
import servicemanager

class read_email():
    def __init__(self):
        # self.app_data_local = shell.SHGetFolderPath(0, shellcon.CSIDL_LOCAL_APPDATA, None, 0)
        # dir_prg_log = os.path.join(self.app_data_local, "elabora_file_picking")
        # if not os.path.exists(dir_prg_log):
        #     os.makedirs(dir_prg_log)

        # logging.basicConfig(filename=os.path.join(dir_prg_log, 'elabora_file_picking.log'), level=logging.DEBUG)
        self.parser = ConfigParser()
        self.parser.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "read_email.ini"))
        self.conn = pymssql.Connection

    def get_mail(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              0xF000, #  Messaggio generico
                              (datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " Controllo casella di posta", ""))

        # logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " Controllo casella di posta")
        return_value = None
        try:
            mail = imaplib.IMAP4_SSL(self.parser.get('email_configuration', 'imap_ssl_server'))
            mail.port = self.parser.get('email_configuration', 'imap_port')
            mail.login(self.parser.get('email_configuration', 'user_email'), self.parser.get('email_configuration',
                                                                                             'pwd_email'))
            mail.select(readonly=0)

            retcode, msgs = mail.search(None, '(UNSEEN)')

            if retcode == 'OK':
                msgs = msgs[0].split()
                if len(msgs) == 0:
                    # logging.info("Nessun messaggio da leggere sul server.")
                    servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                                          0xF000,  # Messaggio generico
                                          (datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                                          + "Nessun messaggio da leggere sul server.", ""))

                for emailid in msgs:
                    resp, data = mail.fetch(emailid, "(RFC822)")
                    email_body = data[0][1]
                    m = email.message_from_string(email_body.decode())
                    email_domain = m["From"].replace("<", "").replace(">", "")

                    if m.get_content_maintype() != 'multipart':
                        continue

                    for part in m.walk():
                        if part.get_content_maintype() == 'multipart':
                            continue
                        if part.get('Content-Disposition') is None:
                            continue

                        filename = part.get_filename()
                        if filename is not None:
                            servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                                                  0xF000,  # Messaggio generico
                                                  (datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                                                   + " Trovato allegato: "
                                                   + filename + ' da ' + email_domain, ""))

                            # logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " Trovato allegato: "
                            #             + filename + ' da ' + email_domain)
                            return {"raw_file": part.get_payload(decode=True), "file_name": filename,
                                    "domain": email_domain}
                            # Nel caso volessimo salvare i file in una cartella decisa
                            # sv_path = os.path.join(svdir, filename)
                            # if not os.path.isfile(sv_path):
                            #    print(sv_path)
                            #    fp = open(sv_path, 'wb')
                            #    fp.write(part.get_payload(decode=True))
                            #    fp.close()
            else:
                servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                                      0xF000,  # Messaggio generico
                                      (datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " controllo posta retcode: "
                                      + retcode, ""))

                # logging.warning(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " controllo posta retcode: " + retcode)
                return return_value
        except:
            servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                                  0xF000,  # Messaggio generico
                                  (datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + str(sys.exc_info()[0]), ""))
            # logging.error("Errore inatteso lettura email: " + str(sys.exc_info()[0]))
            return return_value

    def write_db_record(self, email_data):
        self.conn = pymssql.connect(self.parser.get('database_configuration', 'host'),
                               self.parser.get('database_configuration', 'user'),
                               self.parser.get('database_configuration', 'password'),
                               self.parser.get('database_configuration', 'database'))
        self.cursor = self.conn.cursor()

        CKYForn = ""
        rag_soc_forn = ""
        self.cursor.execute("""
            SELECT
                SINFCKY_CNT, CDS_CNT_RAGSOC
            FROM
                TINF_INDIRIZZIFORNITORI
                INNER JOIN %s%s_rudt ON (CKY_CNT = SINFCKY_CNT)
            WHERE
                sInfDomForn = '%s'""" %
                       (self.parser.get('database_configuration', 'database_mexal'),
                       self.parser.get('database_configuration', 'prefix_mexal'), email_data["domain"].split("@")[1]))
        row = self.cursor.fetchone()
        if row:
            CKYForn = row[0]
            rag_soc_forn = row[1]

        try:
            s_file = email_data["raw_file"].decode().replace("'", "")
        except:
            import binascii
            s_file = '0x' + binascii.hexlify(email_data["raw_file"]).decode('ascii')

        self.cursor.execute(("""INSERT [TFBL_FILEBOLLE]
           ([VFBLFILEDATA], [SFBLBOLLA], [SFBLFORNITORE], [SFBLCKY_CNT], [DFBLDATABOLLA])
           VALUES
           (Convert(varbinary, '%s'), '%s', '%s', '%s', '%s')
           """ % (s_file, email_data["file_name"], rag_soc_forn, CKYForn, datetime.datetime.now().strftime("%Y-%m-%d")))
                            )
        last_row_id = self.cursor.lastrowid

        try:
            return_list = []
            if CKYForn == "341.00118":
                return_list = self.write_dtl_00118(last_row_id, email_data)

            if CKYForn == "341.00031":
                return_list = self.write_dtl_00031(last_row_id, email_data)

            if CKYForn == "341.00032":
                return_list = self.write_dtl_00032(last_row_id, email_data)

            if CKYForn == "341.00034":
                return_list = self.write_dtl_00034(last_row_id, email_data)

            if CKYForn == "341.00420":
                return_list = self.write_dtl_00420(last_row_id, email_data)

            if CKYForn == "341.00393":
                return_list = self.write_dtl_00393(last_row_id, email_data)

            if return_list:
                # Aggiorno i dati del record di testata con le info lette dal file: numero bolla e data bolla
                self.cursor.execute(("""UPDATE TFbl_FileBolle set sFblBolla = '{0}', dFblDataBolla = '{1}',
                    dFblDataElab = '{2}' WHERE iFblId = {3} """).format(*return_list))
            else:
                servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                                      0xF000,  # Messaggio generico
                                      (datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + "EMail sconosciuta!", ""))
                # logging.error(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + "EMail sconosciuta!")

                self.conn.commit()

        except:
            servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                                  0xF000,  # Messaggio generico
                                  (datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + str(sys.exc_info()[0]), ""))
            # logging.error(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + "Errore inatteso scrittura dati: "
            # + str(sys.exc_info()[0]))

        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              0xF000,  # Messaggio generico
                              (datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " Chiusura ...", ""))
        # logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " Chiusura ...")
        self.conn.close()

    def is_number(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def write_dtl_00031(self, id_fbl, email_data):
        # ************ NEW FORM utilizza file csv
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              0xF000,  # Messaggio generico
                              (datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " write_dtl_00031", ""))

        # logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " write_dtl_00031")

        num_bolla = 0

        for file_row in email_data["raw_file"].decode().split("\n"):
            riga_lista = file_row.replace("\n", "").replace("\r", "").split(";")
            if len(riga_lista) >= 5:
                qta = riga_lista[5].replace(",", ".")
                if self.is_number(qta):
                    data_bolla = riga_lista[1].replace('"', '')
                    num_bolla = riga_lista[0].replace('"', '')
                    cod_art = riga_lista[3].replace(".", "").replace('"', '')
                    self.cursor.execute(
                        "INSERT TDtb_DettaglioBolle (iFblId, sDtbCodArt, fDtbQta) VALUES (%d, '%s', %d)"
                        %(id_fbl, cod_art, float(qta)))
        return [num_bolla, datetime.datetime.strptime(data_bolla, "%Y%m%d").strftime("%Y-%m-%d"),
                datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S"), id_fbl]


    def write_dtl_00032(self, id_fbl, email_data):
        # ************ FALMEC utilizza file txt
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              0xF000,  # Messaggio generico
                              (datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " write_dtl_00032", ""))
        # logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " write_dtl_00032")
        fieldwidths = (4, -2, 8, -5, 4, 23, -23, 6, )  # I campi valori rappresentano i campi da ignorare.
        fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's') for fw in fieldwidths)
        fieldstruct = struct.Struct(fmtstring)
        parse = fieldstruct.unpack_from

        num_bolla = 0
        data_bolla = datetime.datetime.now().strftime("%d%m%Y")

        for file_row in email_data["raw_file"].decode().split("\n"):
            if len(file_row) > 100:
                fields = parse(file_row.encode())
                qta = fields[2].decode()
                if self.is_number(qta):
                    num_bolla = fields[0].decode()
                    data_bolla = fields[1].decode()
                    cod_art = fields[3].decode()
                    if self.is_number(qta):
                        self.cursor.execute(
                            "INSERT TDtb_DettaglioBolle (iFblId, sDtbCodArt, fDtbQta) VALUES (%d, '%s', %d)"
                            %(id_fbl, cod_art, float(qta)))

        return [num_bolla, datetime.datetime.strptime(data_bolla, "%d%m%Y").strftime("%Y-%m-%d"),
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), id_fbl]


    def write_dtl_00034(self, id_fbl, email_data):
        # ************ ELMI utilizza file xls
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              0xF000,  # Messaggio generico
                              (datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " write_dtl_00034", ""))
        # logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " write_dtl_00034")
        import xlrd

        num_bolla = 0
        data_bolla = datetime.datetime.now().strftime("%d/%m/%Y")

        xl_workbook = xlrd.open_workbook(file_contents=email_data["raw_file"])
        xl_sheet = xl_workbook.sheet_by_index(0)
        for row in xl_sheet.get_rows():
            qta = row[12].value
            if self.is_number(qta):
                num_bolla = row[1].value
                cod_art = row[9].value
                data_bolla = row[2].value
                self.cursor.execute(
                    "INSERT TDtb_DettaglioBolle (iFblId, sDtbCodArt, fDtbQta) VALUES (%d, '%s', %d)"
                    % (id_fbl, cod_art, float(qta)))

        return [num_bolla, datetime.datetime.strptime(str(data_bolla), "%d/%m/%Y").strftime("%Y-%m-%d"),
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), id_fbl]

    def write_dtl_00118(self, id_fbl, email_data):
        # ************ BOSCH utilizza file csv
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              0xF000,  # Messaggio generico
                              (datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " write_dtl_00018", ""))
        # logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " write_dtl_00018")

        num_bolla = 0
        data_bolla = datetime.datetime.now().strftime("%d/%m/%Y")

        for file_row in email_data["raw_file"].decode().split("\n"):
            riga_lista = file_row.replace("\n", "").replace("\r", "").split(";")
            if len(riga_lista.strip()) >= 5:
                qta = riga_lista[3]
                if self.is_number(qta):
                    num_bolla = riga_lista[0]
                    cod_art = riga_lista[1]
                    self.cursor.execute(
                        "INSERT TDtb_DettaglioBolle (iFblId, sDtbCodArt, fDtbQta) VALUES (%d, '%s', %d)"
                        %(id_fbl, cod_art, float(qta)))
        return [num_bolla, "", datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S"), id_fbl]


    def write_dtl_00393(self, id_fbl, email_data):
        # ************ WHIRPOOL utilizza file txt separato da ;
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              0xF000,  # Messaggio generico
                              (datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " write_dtl_00393", ""))
        # logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " write_dtl_00393")

        num_bolla = 0
        data_bolla = datetime.datetime.now().strftime("%Y%m%d")

        for file_row in email_data["raw_file"].decode().split("\n"):
            riga_lista = file_row.split(";")
            # Filtro le righe spuria
            if len(riga_lista) >= 19:
                qta = riga_lista[19]
                if self.is_number(qta):
                    num_bolla = riga_lista[0].strip()
                    cod_art = riga_lista[18].replace("/", "").strip()
                    data_bolla = riga_lista[1].strip()

                    self.cursor.execute(
                        "INSERT TDtb_DettaglioBolle (iFblId, sDtbCodArt, fDtbQta) VALUES (%d, '%s', %d)"
                        % (id_fbl, cod_art, float(qta)))
        return [num_bolla, datetime.datetime.strptime(str(data_bolla), "%Y%m%d").strftime("%Y-%m-%d"),
                datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S"), id_fbl]


    def write_dtl_00420(self, id_fbl, email_data):
        # ************ VIBO utilizza foglio excel!
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              0xF000,  # Messaggio generico
                              (datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " write_dtl_00420", ""))
        # logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " write_dtl_00420")

        num_bolla = 0
        data_bolla = datetime.datetime.now().strftime("%Y%m%d")

        import xlrd

        xl_workbook = xlrd.open_workbook(file_contents=email_data["raw_file"])
        xl_sheet = xl_workbook.sheet_by_index(0)
        for row in xl_sheet.get_rows():
            # Considero soltanto le righe di tipo R. Assumo che le rige D siano descrizioni
            if row[69].value == "R":
                num_bolla = row[4].value
                data_bolla = ''.join([str(s) for s in xlrd.xldate_as_tuple(row[6].value, 0) if s != 0])
                qta = row[50].value
                cod_art = row[47].value
                if self.is_number(qta):
                    self.cursor.execute(
                        "INSERT TDtb_DettaglioBolle (iFblId, sDtbCodArt, fDtbQta) VALUES (%d, '%s', %d)"
                        %(id_fbl, cod_art, float(qta)))

        return [num_bolla, datetime.datetime.strptime(str(data_bolla), "%Y%m%d").strftime("%Y-%m-%d"),
                datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S"), id_fbl]

re = read_email()
email_data = re.get_mail()
while email_data:
    re.write_db_record(email_data)
    email_data = re.get_mail()
