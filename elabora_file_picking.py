import win32serviceutil
import win32service
import win32event
import win32api
import servicemanager
from read_email import read_email
import datetime


class AppServerSvc(win32serviceutil.ServiceFramework):
    _svc_name_ = "elabora_file_picking"
    _svc_display_name_ = "elabora_file_picking"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

        # socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        rc = None
        date_now = datetime.datetime.now()
        while rc != win32event.WAIT_OBJECT_0:
            try:
                print(1/0)
                # Per impostazione predefinita leggo la mail ogni 10 minuti. Caso mai modificare o portare a parametro.
                if datetime.datetime.now() - date_now > datetime.timedelta(minutes=10):
                    date_now = datetime.datetime.now()
                    re = read_email()
                    email_data = re.get_mail()
                    while email_data:
                        re.write_db_record(email_data)
                        email_data = re.get_mail()
            except:
                servicemanager.LogMsg(servicemanager.EVENTLOG_ERROR_TYPE, servicemanager.PYS_SERVICE_STARTED,
                                      (self._svc_name_, ''))
            rc = win32event.WaitForSingleObject(self.hWaitStop, 5000)

    def main(self):
        pass

def ctrlHandler(ctrlType):
    return True

if __name__ == '__main__':
    win32api.SetConsoleCtrlHandler(ctrlHandler, True)
    win32serviceutil.HandleCommandLine(AppServerSvc)
