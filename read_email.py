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
from send_email_info import send_email

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
        self.good_extensions = ['TXT', 'CSV', 'XLS', 'XLSX']
        self.msg_to_send = ""

    def get_mail(self):
        self.msg_to_send = ""
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
                        try:
                            self.good_extensions.index(filename[-3:].upper())
                        except:
                            continue
                        if filename is not None:
                            servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                                                  0xF000,  # Messaggio generico
                                                  (datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                                                   + " Trovato allegato: "
                                                   + filename + ' da ' + email_domain, ""))

                            # logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + " Trovato allegato: "
                            #             + filename + ' da ' + email_domain)
                            try:
                                local_raw_file =  part.get_payload(decode=True)
                            except:
                                servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                                                      0xF000,  # Messaggio generico
                                                      (datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                                                       + " Scartato allegato: "
                                                       + filename + ' da ' + email_domain, ""))

                            return {"subject": m["subject"], "raw_file": local_raw_file, "file_name": filename,
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
        # Gestione della decodifica del subject: se inizia per *** e subito dopo c'è un codice fornitore, prendo quello.
        if email_data["subject"].startswith("***"):
            CKYForn = email_data["subject"].replace("***", "").strip()
        if CKYForn == "":
            self.cursor.execute("""
                SELECT
                    SINFCKY_CNT, CDS_CNT_RAGSOC
                FROM
                    TINF_INDIRIZZIFORNITORI
                    INNER JOIN %s%srudt ON (CKY_CNT = SINFCKY_CNT)
                WHERE
                    sInfDomForn = '%s'""" %(self.parser.get('database_configuration', 'database_mexal'),
                                            self.parser.get('database_configuration', 'prefix_mexal'),
                                            email_data["domain"].split("@")[1]))
            row = self.cursor.fetchone()
            if row:
                CKYForn = row[0]
                rag_soc_forn = row[1]
        else:
            self.cursor.execute("""
                SELECT
                    CDS_CNT_RAGSOC
                FROM
                    %s%srudt
                WHERE
                    CKY_CNT = '%s'
            """ % (self.parser.get('database_configuration', 'database_mexal'),
                   self.parser.get('database_configuration', 'prefix_mexal'), CKYForn))
            row = self.cursor.fetchone()
            if row:
                rag_soc_forn = row[0]

        # Se il file è excel decodifico subito in ascii con binascii.hexlify
        if (email_data["file_name"][-3:].upper() != 'XLS') & (email_data["file_name"][-3:].upper() != 'XLSX'):
            try:
                try:  # Se il file non contiene caratteri particolari (eg. °)
                    s_file = email_data["raw_file"].decode().replace("'", "")
                except: # Altrimenti li gestisco con latin-1
                    s_file = email_data["raw_file"].decode("latin-1").replace("'", "")
            except:
                import binascii
                s_file = '0x' + binascii.hexlify(email_data["raw_file"]).decode('ascii')
        else:
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

            if self.msg_to_send == "":
                self.conn.commit()
            else:
                send_mail_obj = send_email()
                send_mail_obj.send_msg("\r\n\r\n".join(["ATTENZIONE: Messaggio non elaborato." +
                                                        "BOLLA: %s Fornitore: %s" % (return_list[0], CKYForn) +
                                                        " CODICI ALIAS NON TROVATI:", self.msg_to_send]))
                self.msg_to_send = ""


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

    def get_ean_from_cod_alias(self, cod_art, cky_forn):
        self.cursor.execute(
            """
            SELECT
              LTrim(RTrim(CSG_ART_ALIAS)) as CSG_ART_ALIAS
            FROM
              $DBMEXAL$$PREFIXTAB$ALIAS
            WHERE
              LTrim(RTrim(CSG_ART_ALIAS)) = '%s'
            """.replace("$DBMEXAL$", self.parser.get('database_configuration', 'database_mexal')).replace(
                "$PREFIXTAB$", self.parser.get('database_configuration', 'prefix_mexal')) % cod_art)

        ret_val = self.cursor.fetchone()
        if ret_val is None:
            self.msg_to_send = "\r\n".join([self.msg_to_send, "Codice file %s, fornitore %s non trovato alias."
                                            % (cod_art, cky_forn)])
        return ret_val


    def get_ean_from_cod_forn(self, cod_art, cky_forn):
        self.cursor.execute(
            """
            SELECT
                LTrim(RTrim(CSG_ART_ALIAS)) as CSG_ART_ALIAS
            FROM
                $DBMEXAL$$PREFIXTAB$ARTM_FOR artmFor
                INNER JOIN $DBMEXAL$$PREFIXTAB$ALIAS alias ON (alias.CKY_ART = artmFor.CKY_ART)
            WHERE
                LTrim(RTrim(CSG_ART_FOR)) = '%s' and artmFor.CKY_CNT_FORN = '%s'
            """.replace("$DBMEXAL$", self.parser.get('database_configuration', 'database_mexal')).replace(
                "$PREFIXTAB$", self.parser.get('database_configuration', 'prefix_mexal')) % (cod_art, cky_forn))

        ret_val = self.cursor.fetchone()
        if ret_val is None:
            self.msg_to_send = "\r\n".join([self.msg_to_send, "Codice file %s, fornitore %s non trovato alias." %(cod_art, cky_forn)])
        return ret_val

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
                    num_bolla = str(int(riga_lista[0].replace('"', '')[-6:]))
                    # Leggo codice EAN dal file e lo scrivo nel database frontiera. Non necessita di alcuna riconversione
                    cod_art = riga_lista[4].replace(".", "").replace('"', '')
                    # In questo caso serve solo per compilare il messaggio mail da inviare altrimenti
                    # mi accorgerei della mancanza del codice soltanto a livello pistole!!
                    self.get_ean_from_cod_alias(cod_art, "341.00031")

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
        #fieldwidths = (4, -2, 8, -9, 23, -13, 6, 6,)  # I campi negativi rappresentano i campi da ignorare.
        fieldwidths = (4, -2, 8, -9, -23, 13, 6, 6,)  # I campi negativi rappresentano i campi da ignorare.
        fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's') for fw in fieldwidths)
        fieldstruct = struct.Struct(fmtstring)
        parse = fieldstruct.unpack_from

        num_bolla = 0
        data_bolla = datetime.datetime.now().strftime("%d%m%Y")

        try:
            data = email_data["raw_file"].decode().split("\n")
        except:
            data = email_data["raw_file"].decode("latin-1").split("\n")
        for file_row in data:
            if len(file_row) > 100:
                fields = parse(file_row.encode())
                qta = fields[3].decode().replace(",", ".")
                if self.is_number(qta):
                    num_bolla = fields[0].decode()[-6:]
                    data_bolla = fields[1].decode()
                    cod_art = fields[2].decode().strip().replace(".", "")
                    if cod_art == "":
                        cod_art = "RICAMBI"
                    self.get_ean_from_cod_alias(cod_art, "341.00032")
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
            qta = row[13].value
            if self.is_number(qta):
                num_bolla = int(row[1].value)
                cod_art = row[9].value

                row_cod_art_ean = self.get_ean_from_cod_forn(cod_art, "341.00034")
                if row_cod_art_ean is not None:
                    cod_art = row_cod_art_ean[0]

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
            if len(riga_lista) >= 5:
                qta = riga_lista[3]
                if self.is_number(qta):
                    num_bolla = riga_lista[0].replace("'", "")[-6:]
                    cod_art = riga_lista[1]
                    row_cod_art_ean = self.get_ean_from_cod_forn(cod_art, "341.00118")
                    if row_cod_art_ean is not None:
                        cod_art = row_cod_art_ean[0]

                    self.cursor.execute(
                        "INSERT TDtb_DettaglioBolle (iFblId, sDtbCodArt, fDtbQta) VALUES (%d, '%s', %d)"
                        %(id_fbl, cod_art, float(qta)))
        # N.B.: il secondo parametro è la data bolla che al momento non mi viene passata nel file. DA GESTIRE.
        return [num_bolla, datetime.datetime.strptime(str(data_bolla), "%d/%m/%Y").strftime("%Y-%m-%d  %H:%M:%S"),
                datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S"), id_fbl]


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
                    num_bolla = riga_lista[0].strip()[-6:]

                    # Leggo codice EAN dal file e lo scrivo nel database frontiera. Non necessita di alcuna riconversione
                    cod_art = riga_lista[16].replace("/", "").strip()
                    # In questo caso serve solo per compilare il messaggio mail da inviare altrimenti
                    # mi accorgerei della mancanza del codice soltanto a livello pistole!!
                    self.get_ean_from_cod_alias(cod_art, "341.00393")

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
                num_bolla = int(row[4].value)
                data_bolla = ''.join([str(s) for s in xlrd.xldate_as_tuple(row[6].value, 0) if s != 0])
                qta = row[50].value
                # Leggo codice EAN dal file e lo scrivo nel database frontiera. Non necessita di alcuna riconversione
                cod_art = row[80].value

                if cod_art[:3] != "400":    # Da quello che vedo i codici che iniziano per 400 sono Imballi.
                    # In questo caso serve solo per compilare il messaggio mail da inviare altrimenti
                    # mi accorgerei della mancanza del codice soltanto a livello pistole!!
                    self.get_ean_from_cod_alias(cod_art, "341.00393")

                    if self.is_number(qta):
                        self.cursor.execute(
                            "INSERT TDtb_DettaglioBolle (iFblId, sDtbCodArt, fDtbQta) VALUES (%d, '%s', %d)"
                            %(id_fbl, cod_art, float(qta)))

        return [num_bolla, datetime.datetime.strptime(str(data_bolla), "%Y%m%d").strftime("%Y-%m-%d"),
                datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S"), id_fbl]

#Debug purpose!!!
#re = read_email()
#email_data = re.get_mail()
#while email_data:
#    re.write_db_record(email_data)
#    email_data = re.get_mail()
