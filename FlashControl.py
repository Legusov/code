import time
import win32api, win32con, win32gui
import win32serviceutil
import win32service
import servicemanager
import threading
import os
import subprocess
from datetime import datetime
import psycopg2
import sys
import wmi
import socket
import re
import psutil
import winreg


GUID_DEVINTERFACE_USB_DEVICE = "{A5DCBF10-6530-11D2-901F-00C04FB951ED}"
WM_DEVICECHANGE = 0x0219

def get_10_ip(ip_in):
    ip = ip_in
    try:
        if ip[:3] != '10.':
            for line in re.findall("'(.*?)'", str(psutil.net_if_addrs())):
                if (re.search("\d+\.\d+\.\d+\.\d+", line)
                        and not re.search("255\.\d+\.\d+\.\d+", line)
                        and not re.search("\d+\.\d+\.\d+\.255", line)
                        and re.search("10\.\d+\.\d+\.\d+", line)
                ):
                    if line:
                        ip = line
    except:
        pass
    return ip

def get_disk_serial_numbers():
    try:
        # Получаем информацию о логических дисках
        logical_disks_command = 'wmic logicaldisk get DeviceID'
        logical_disks_output = os.popen(logical_disks_command).read()

        # Получаем информацию о серийных номерах
        disk_info_command = 'wmic diskdrive get DeviceID,SerialNumber /format:list'
        disk_info_output = os.popen(disk_info_command).read()

        # Обрабатываем вывод
        logical_disks = [line.strip() for line in logical_disks_output.split('\n') if line.strip()]
        disk_info_lines = disk_info_output.strip().split('\n\n')
        # print(logical_disks, disk_info_lines)

        disk_info_dict = {}
        for disk_info_block in disk_info_lines:
            disk_info = dict(line.split('=') for line in disk_info_block.strip().split('\n'))
            print(disk_info)
            device_id = disk_info.get('DeviceID')
            serial_number = disk_info.get('SerialNumber')
            print(device_id, serial_number)
            # if device_id and serial_number:
            for logical_disk in logical_disks:
                print(logical_disk, serial_number.strip())
                if device_id in logical_disk:
                    disk_info_dict[logical_disk] = serial_number.strip()

        return disk_info_dict
    except Exception as e:
        print(f"Ошибка: {e}")
        return None

def get_usb_serial_number():
    result = os.popen('wmic diskdrive get serialnumber').read().split('\n\n')[1:]
    output = []
    for item in result:
        if item:
            output.append(item.strip())
    return output

def message_handler(hwnd, msg, wparam, lparam):
    global curr_surf
    global curr_letter
    global color_windows
    computer_name = os.environ['COMPUTERNAME']
    #ip_address = socket.gethostbyname(socket.gethostname())
    ip_address = get_10_ip(socket.gethostbyname(socket.gethostname()))
    time = datetime.now()

    if wparam == win32con.DBT_DEVICEARRIVAL and msg == WM_DEVICECHANGE:

        new_surf, new_letter = get_id_serialnumber()
        drive = list(set(new_surf) - set(curr_surf))[0]
        letter = [item for item in new_letter if item not in curr_letter][0]
        curr_surf, curr_letter = new_surf, new_letter
        serial_number = new_surf[drive]
        print(f'serial_number: {serial_number}\n'
              f'new_surf: {new_surf}\n'
              f'new_letter: {new_letter}\n')


        tt = f'powershell -command "tree {letter}\\ /f /a > {os.getcwd()}\\{serial_number}.txt"'
        os.system(tt)
        file = serial_number + ".txt"
        dtime = time.strftime('%Y-%m-%d %H:%M:%S')
        vtiem = time.strftime('%H:%M:%S')
        #users = '; '.join(get_all_users())
        log_user = get_current_user()
        log_user_os = get_current_user_os()

        a = f'IN serial_number: {serial_number}\nIP: {ip_address}\nNAME_PC: {computer_name}\nUSER: {log_user} {log_user_os}\n-----------------------------\n'

        with open(f'{work_dir}\\{service_name}\\{serial_number}.txt', "a") as file_in_dir:
            file_in_dir.write(a)


        #print(type(serial_number))
        # conn = psycopg2.connect(
        #     dbname='postgres',
        #     user='postgres',
        #     password='Metro575816',
        #     host='10.238.1.131',
        #     port='5432'
        # )
        # #print("123")
        # cursor = conn.cursor()
        # cursor.execute(
        #     "INSERT INTO \"Flash_data_input\" (serial_number, data_input) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        #         (serial_number, ''))
        # conn.commit()
        #
        # cursor = conn.cursor()
        # with open(file,'r',encoding="utf-16-le") as f:
        #     cursor.execute(
        #         "INSERT INTO \"data_all\" (serial_number, ip_address, pc_name, data, time, files) VALUES (%s, %s, %s, %s, %s ,%s)",
        #         (serial_number, ip_address, computer_name, dtime, vtiem, f.read()))
        #     #print("Инфа по файлам записана")
        #     cursor.execute(
        #         'INSERT INTO users_info (pc_name, all_users) values (%s, %s) on conflict (pc_name) do update set all_users = %s',
        #         (computer_name, users, users))
        #     conn.commit()
        #     conn.close()
        #     #print("Лок админ записан")
        #os.remove(file)

    elif wparam == win32con.DBT_DEVICEREMOVECOMPLETE and msg == WM_DEVICECHANGE:

        new_surf, new_letter = get_id_serialnumber()
        serial_number = curr_surf[list(set(curr_surf) - set(new_surf))[0]]

        a = f'OUT serial_number: {serial_number}\n'

        with open(f'{work_dir}\\{service_name}\\{serial_number}.txt', "a") as file_in_dir:
            file_in_dir.write(a)
        letter = [item for item in curr_letter if item not in new_letter][0]
        curr_surf, curr_letter = new_surf, new_letter
        #print(f"Removed USB Device Serial Number and Letter: {serial_number}, '{letter}'")

    return True

def create_window():
    wc = win32gui.WNDCLASS()
    wc.lpfnWndProc = message_handler
    wc.lpszClassName = "USBDeviceMonitor"
    hinst = wc.hInstance = win32api.GetModuleHandle(None)
    class_atom = win32gui.RegisterClass(wc)
    hwnd = win32gui.CreateWindow(
        class_atom, "USB Device Monitor", win32con.WS_ICONIC, 0, 0, win32con.CW_USEDEFAULT, 0,
        0, 0, hinst, None
    )
    win32gui.PumpMessages()

def get_id_serialnumber():
    a = os.popen('wmic diskdrive get DeviceID, SerialNumber').read().split('\n\n')[1:]
    b = os.popen('wmic logicaldisk get DeviceID').read().split('\n\n')[1:]
    data = [value.strip() for value in a if value]
    data1 = [value.strip() for value in b if value]
    drive_dict = {}

    for item in data:
        parts = item.split()
        if len(parts) == 2:
            drive_dict[parts[0]] = parts[1]
    return drive_dict, data1

def get_files_in_directory(directory):
    file_list = []
    if os.path.exists(directory) and os.path.isdir(directory):
        for root, dirs, files in os.walk(directory):
            file_list.extend(files)
        return '; '.join(file_list)
    return None


def get_current_user_os():
    try:
        key = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\Microsoft\Windows\CurrentVersion\Authentication\LogonUI"
        )
        value, _ = winreg.QueryValueEx(key, "LastLoggedOnUser")
        if '\\' in value:
            return value.split('\\')[1]
        else:
            return value
    except:
        return None

def get_current_user():
    try:
        return win32api.GetUserName()
    except:
        return None


def get_all_users():
    users_path = 'C://Users'
    special_folders = ['All Users', 'Default', 'Default User', 'Public', 'Все пользователи']

    users = [name for name in os.listdir(users_path)
             if os.path.isdir(os.path.join(users_path, name)) and name not in special_folders]

    current_user = get_current_user()

    result = []
    for user in users:
        result.append({
            'name': user,
            'is_current': (user == current_user)
        })
    return result

# def get_all_users():
#     users_path = 'C://Users'
#     special_folders = ['All Users', 'Default', 'Default User', 'Public', 'Все пользователи']
#
#     users = [name for name in os.listdir(users_path)
#              if os.path.isdir(os.path.join(users_path, name)) and name not in special_folders]
#
#     result = []
#     for user in users:
#         result.append(user)
#     return result

# Часть кода отвечающего за создание, запуск и удаление службы---------------------------------------------------------

def get_service_status(service_name):
    try:
        wmiobj = wmi.WMI()
        services = wmiobj.Win32_Service(Name = service_name)
        return services[0].state
    except:
        return False

def get_current_folder():
    if getattr(sys, 'frozen', False):
        # Если запущен как .exe (PyInstaller)
        print(os.path.dirname(sys.executable))
        return os.path.dirname(sys.executable)
    else:
        # Если запущен как скрипт Python
        print(os.path.dirname(os.path.abspath(__file__)))
        return os.path.dirname(os.path.abspath(__file__))

current_folder = get_current_folder()

def copy_file():
    if not os.path.exists(f'{work_dir}\\FlashControl'):
        try:
            print(work_dir, current_folder)
            os.popen(f'mkdir "{work_dir}\\FlashControl"')
            #os.popen(f'xcopy "{sys._MEIPASS}\\files\\FlashControl\\" "{work_dir}\\FlashControl" /e')
            os.popen(f'xcopy "{current_folder}" "{work_dir}\\FlashControl" /e')

            # os.system(f'mkdir "{work_dir}\\FlashControl" >NUL 2>&1')
            # os.system(f'copy {sys.argv[0]} "{work_dir}\\FlashControl\\{service_name}.exe" >NUL 2>&1')
            return True
        except:
            print("Недостаточно прав для создания рабочей директории")
            return False
    else:
        os.popen(f'xcopy "{current_folder}" "{work_dir}\\FlashControl" /e')


def execute_powershell_with_logging(command, log_file):
    """Выполняет PowerShell команду с логированием"""
    try:
        # Используем системную кодировку для вывода консоли
        import locale
        system_encoding = locale.getpreferredencoding()  # Обычно 'cp1251' в русской Windows

        result = subprocess.run(['where', 'powershell'], capture_output=True, text=True)

        if result.returncode == 0:
            ps_exe = result.stdout.strip().split('\n')[0]
        else:
            ps_exe = 'powershell.exe'

        process = subprocess.Popen(
            [f'{ps_exe}', '-Command', command],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding=system_encoding  # ← 'cp1251' для русской Windows
        )
        stdout, stderr = process.communicate(timeout=30)
        returncode = process.returncode

        # Сохраняем в лог в UTF-8 (для универсальности)
        with open(f'C:\\Program Files\\FlashControl\\ok_{log_file}', 'w', encoding='utf-8') as f:
            f.write(f"=== {datetime.now()} ===\n")
            f.write(f"Command: {command}\n")
            f.write(f"Return Code: {returncode}\n")
            f.write("=" * 60 + "\n")
            if stdout:
                f.write(stdout + "\n")
            if stderr:
                f.write("ERROR:\n" + stderr + "\n")

        return returncode, stdout, stderr

    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
        return -1, "", "Timeout"
    except Exception as e:
        with open(f'C:\\Program Files\\FlashControl\\err_{log_file}', 'w', encoding='utf-8') as f:
            f.write(f"ERROR: {str(e)}\n")
        return -1, "", str(e)

def install_service(service_name):
        # if get_service_status(service_name):
        #     command_start = f'sc start FlashControl'
        #     os.system(command_start)
        result = subprocess.run(['where', 'sc'], capture_output=True, text=True)

        if result.returncode == 0:
            sc_path = result.stdout.strip().split('\n')[0]
        else:
            sc_path = r'C:\Windows\System32\sc.exe'

        com = f'{sc_path} create FlashControl binpath="{work_dir}\\FlashControl\\{service_name}.exe"'
        os.system(com)
        os.system(f'{sc_path} config FlashControl start=auto')
        command_description = f'{sc_path} description FlashControl "Мониторинг подключаемых USB носителей"'
        os.system(command_description)
        command_config = f'{sc_path} config FlashControl DisplayName="Служба контроля съемных USB носителей"'
        os.system(command_config)
        os.system(f'{sc_path} failure "FlashControl" reset= 86400 actions= restart/20000/restart/40000/restart/60000')
        # command_start = f'sc start FlashControl'
        # os.system(command_start)
        try:
            ps_com = f'Start-Sleep 5; {sc_path} start FlashControl'
            returncode, stdout, stderr = execute_powershell_with_logging(ps_com, 'flash_control.log')

            if returncode == 0:
                print("Команда выполнена успешно. Лог сохранен в flash_control.log")
            else:
                print(f"Ошибка выполнения. Код: {returncode}. Подробности в flash_control.log")
        except Exception as e:
            try:
                with open(f'{work_dir}\\FlashControl\\err.txt', 'w') as f:
                    f.write(str(e))
            except:
                pass
        # try:
        #     ps_com = 'Start-Sleep 5; sc.exe start FlashControl'
        #     #subprocess.Popen(['powershell.exe', '-Command', ps_com])
        #     returncode, stdout, stderr = execute_powershell_with_logging(ps_com, 'flash_control.log')
        #
        #     if returncode == 0:
        #         print("Команда выполнена успешно. Лог сохранен в flash_control.log")
        #     else:
        #         print(f"Ошибка выполнения. Код: {returncode}. Подробности в flash_control.log")
        # except Exception as e:
        #     try:
        #         with open(f'{work_dir}\\FlashControl\\err5.txt', 'w') as f:
        #             f.write(str(e))
        #     except:
        #         pass
        # ps_command = '$res = 0; for(;;) {Start-Sleep -s 20; $stat = (Get-Service FlashControl).Status; if ($stat -eq "Stopped") {Start-Service -Name FlashControl; $res = 1} elseif ($stat -eq "Running") {break} else {break} }'
        # subprocess.Popen(['powershell.exe', '-Command', ps_command])

        # subprocess.Popen([
        #     'powershell.exe',
        #     '-Command',
        #     'sc.exe sdset FlashControl "D:(A;;CCLCSWRPWPDTLOCRRC;;;SY)(A;;CCDCLCSWRPWPDTLOCRSDRCWDWO;;;BA)(A;;CCLCSWLOCRRC;;;IU)"'
        # ])
        #
        # # Комплексный запуск с таймаутом
        # start_script = """
        # $service = Get-Service -Name "FlashControl"
        # if ($service.Status -ne 'Running') {
        #     # Увеличиваем таймаут через реестр
        #     Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control" -Name "ServicesPipeTimeout" -Value 60000 -Type DWORD
        #
        #     # Запускаем службу с повторными попытками
        #     $retryCount = 3
        #     $retryDelay = 5
        #
        #     for ($i = 1; $i -le $retryCount; $i++) {
        #         try {
        #             Start-Service $service -ErrorAction Stop
        #             Write-Host "Служба успешно запущена (попытка $i)"
        #             break
        #         } catch {
        #             Write-Host "Ошибка при попытке $i : $_"
        #             if ($i -lt $retryCount) {
        #                 Start-Sleep -Seconds $retryDelay
        #                 # Дополнительные действия перед повторной попыткой
        #                 sc.exe config FlashControl start= delayed-auto
        #                 sc.exe failure FlashControl reset= 60 actions= restart/5000
        #             }
        #         }
        #     }
        # }
        # """
        # subprocess.Popen(['powershell.exe', '-Command', start_script])

        # ps_com = 'sc.exe sdset FlashControl "D:(A;;CCLCSWRPWPDTLOCRRC;;;SY)(A;;CCDCLCSWRPWPDTLOCRSDRCWDWO;;;BA)(A;;CCLCSWLOCRRC;;;IU)"'
        # subprocess.Popen(['powershell.exe', '-Command', ps_com])
        # ps_com = "Start-Sleep 5; sc.exe start FlashControl"
        # #ps_com = """Start-Sleep -Seconds 5; Start-Process powershell -Verb RunAs -ArgumentList "Start-Service -Name 'FlashControl' -ErrorAction Stop" """
        # subprocess.Popen(['powershell.exe', '-Command', ps_com])

def remove_service(service_name):
    try:
        result = subprocess.run(['where', 'sc'], capture_output=True, text=True)

        if result.returncode == 0:
            sc_path = result.stdout.strip().split('\n')[0]
        else:
            sc_path = r'C:\Windows\System32\sc.exe'
        command_stop = f'{sc_path} stop FlashControl >NUL 2>&1'
        os.system(command_stop)
        command_remove = f'{sc_path} delete FlashControl >NUL 2>&1'
        os.system(command_remove)
        print(f"Служба '{service_name}' удалена...")
    except:
        print("Недостаточно прав для остановки и удаления службы...")
    # key_no = False
    # status = get_service_status(service_name)
    # if not status:
    #     key_no = True
    #     try:
    #         if os.path.exists(f'{work_dir}\\FlashControl'):
    #             os.system(f'rmdir /s /q "{work_dir}\\FlashControl" >NUL 2>&1')
    #     except:
    #         print("Недостаточно прав для остановки и удаления службы...")
    # else:
    #     try:
    #         command_stop = f'sc stop FlashControl >NUL 2>&1'
    #         os.system(command_stop)
    #         command_remove = f'sc delete FlashControl >NUL 2>&1'
    #         os.system(command_remove)
    #         os.system(f'rmdir /s /q "{work_dir}\\FlashControl" >NUL 2>&1')
    #     except:
    #         print("Недостаточно прав для остановки и удаления службы...")
    #
    # status = get_service_status(service_name)
    # if not status and not key_no:
    #     write_stat_db("del")
    #     print(f"Служба '{service_name}' удалена...")

def init():
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(MyServiceFramework)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(MyServiceFramework)

def check_version(ver):
    update = False
    conn = psycopg2.connect(
        dbname='Portal',
        user='postgres',
        password='Metro575816',
        host='10.238.1.131',
        port='5432'
    )

    cursor = conn.cursor()
    cursor.execute(f"SELECT name, file_test  FROM questions where id = '36'")
    # colnames = [desc[0] for desc in cursor.description]
    # print(colnames)
    data = cursor.fetchone()
    print(data[0][-1])
    print(data[1])
    if int(data[0][-1]) > int(ver):
        update = True
        with open(f'{work_dir}\\FlashControl\\update.zip', 'wb') as f:
            f.write(data[1])
    cursor.close()
    conn.close()
    #print("Обновление загружено")
    if os.path.isfile(f'{work_dir}\\FlashControl\\update.zip') and update:
        ps_command = 'Stop-Service -Name FlashControl; Expand-Archive -Path "C:\\Program Files\\FlashControl\\update.zip" -DestinationPath "C:\\Program Files\\FlashControl" -Force; Start-Service -Name FlashControl; Remove-Item -Path "C:\\Program Files\\FlashControl\\update.zip"'
        subprocess.Popen(['powershell.exe', '-Command', ps_command])

def write_stat_db(key):
    name = os.getenv('COMPUTERNAME', 'defaultValue')
    if key == "del":
        try:
            # ip_addr = socket.gethostbyname(socket.gethostname())
            ip_addr = get_10_ip(socket.gethostbyname(socket.gethostname()))
            now = datetime.now()
            date_all = now.strftime('%Y-%m-%d %H:%M:%S')
            conn = psycopg2.connect(
                dbname='postgres',
                user='postgres',
                password='Metro575816',
                host='10.238.1.131',
                port='5432'
            )
            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE flash_accept SET ip = '{ip_addr}', date_del = '{date_all}', flag = False, online = False where pc_name = '{name}'")
            conn.commit()
            conn.close()
        except:
            pass
    elif key == "start":
        while True:
            try:
                # ip_addr = socket.gethostbyname(socket.gethostname())
                ip_addr = get_10_ip(socket.gethostbyname(socket.gethostname()))
                now = datetime.now()
                date_now = now.strftime('%Y-%m-%d')
                time_now = now.strftime('%H:%M:%S')
                date_all = now.strftime('%Y-%m-%d %H:%M:%S')
                conn = psycopg2.connect(
                    dbname='postgres',
                    user='postgres',
                    password='Metro575816',
                    host='10.238.1.131',
                    port='5432'
                )
                cursor = conn.cursor()
                cursor.execute(f"SELECT pc_name FROM flash_accept WHERE pc_name = '{name}' LIMIT 1")
                if cursor.fetchone():
                    print("UPDATE")
                    cursor.execute(f"UPDATE flash_accept SET ip = %s, date = %s, time = %s, flag = %s, date_del = %s, online = %s  where pc_name = '{name}'",
                                   (ip_addr, date_now, time_now, True, None, True))
                else:
                    print("NEW RECORD")
                    cursor.execute(
                        "INSERT INTO \"flash_accept\" (pc_name, ip, date, time, flag, date_start, date_del, online) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        (name, ip_addr, date_now, time_now, True, date_all, None, True))
                conn.commit()
                conn.close()

                #check_version(ver)
                time.sleep(1800)

            except:
                time.sleep(60)


class MyService:

    def stop(self):
        self.running = False
        servicemanager.LogInfoMsg(f"Service stop...")

    # Запуск логики отправки сообщения
    def run(self):
        global curr_surf, curr_letter, running
        self.running = True

        servicemanager.LogInfoMsg(f"Service running...")
        # ps_command = '$res = 0; $stop = 0; for(;;) {Start-Sleep -s 10; $stat = (Get-Service FlashControl).Status; if ($stat -eq "Stopped") {Start-Service -Name FlashControl; $res = 1} elseif ($stat -eq "Running") {if ($res -eq 1 -Or $stop -eq 30) {break} else {Start-Sleep -s 10; $stop++ }} else {break} }'
        # subprocess.Popen(['powershell.exe', '-Command', ps_command])
        curr_surf, curr_letter = get_id_serialnumber()
        threading.Thread(target=create_window).start()
        #threading.Thread(target=write_stat_db, args=("start",)).start()
        while self.running:
            time.sleep(10)

class MyServiceFramework(win32serviceutil.ServiceFramework):
    _svc_name_ = 'FlashControl'
    _svc_display_name_ = 'Служба контроля съемных USB носителей'
    _svc_description_ = "Мониторинг подключаемых USB носителей"

    # Остановка сервиса
    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.service_impl.stop()
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)
    # Запуск сервиса
    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
        self.service_impl = MyService()
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        self.service_impl.run()

work_dir = os.environ["WINDIR"].split('\\')[0]+"\\Program Files"
service_name = 'FlashControl'
ver = 5

if __name__ == "__main__":
    # try:
    #     with open(f'{work_dir}\\FlashControl\\ver.txt', 'w') as f:
    #         f.write(str(ver))
    # except:
    #     pass

    try:
        if sys.argv[1] == "run":
            copy_file()
            install_service(service_name)
        elif sys.argv[1] == "del":
            #copy_file()
            remove_service(service_name)
            try:
                ps_command = 'Start-Sleep -Seconds 5; Remove-Item -Path "C:\\Program Files\\FlashControl" -Recurse -Force'
                subprocess.Popen(['powershell.exe', '-Command', ps_command])
            except:
                pass

            # try:
            #     if os.path.exists(f'C:\\Program Files\\FlashControl'):
            #         os.system(f'rmdir /s /q "C:\\Program Files\\FlashControl" >NUL 2>&1')
            # except:
            #     pass
            #remove_service(service_name)
        else: init()
    except:
        init()

