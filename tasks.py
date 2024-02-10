import os
import subprocess
from robocorp.tasks import task
from RPA.Robocorp.Storage import Storage
from RPA.Robocorp.Vault import Vault
import cdata.quickbooks as mod

storage = Storage()
def install_license():
    _secret = Vault().get_secret("CData")
    license_key = _secret["License"]
    original_directory = os.getcwd()
    installer_path='install-license.exe'
    os.chdir("./libraries/cdata/installlic_quickbooks")
    # Example command (replace this with the actual path to install-license.exe and the license key)
    command = [installer_path, license_key]
    # Open a subprocess and establish communication
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    # If the subprocess prompts for input, you can provide it here
    input_data = "Mark Klagenberg\nklagster@gmail\n"
    output, error = process.communicate(input=input_data)
    # Print the output and error
    print(f"Output:, {output}")
    print(f"Error:, {error}")
    # Wait for the subprocess to complete (optional)
    process.wait()
    os.chdir(original_directory)

@task
def delete_all_qbd_bills_and_credits():
    _secret = Vault().get_secret("Quickbooks")
    UNAME = _secret["User"]
    PWORD = _secret["Password"]
    HOST  = _secret["Host"]
    BATCH = int(storage.get_text_asset("QBDBatchSize"))
                    
    install_license()
    #quickbooks_connection_string= f"User={UNAME};Password={PWORD};URL={HOST}"
    quickbooks_connection_string= f"User={UNAME};Password=Smeghead1!;URL={HOST}"
    connn = mod.connect(quickbooks_connection_string)
    cur = connn.cursor()

    all_bills = connn.execute("SELECT ID FROM Bills;").fetchall()
    for i in range(0, len(all_bills), BATCH):
        sub_list = all_bills[i:i+BATCH]  # Create a sublist of 5 rows or the remaining rows
        param_values = [(item[0],) for item in sub_list]
        delete_command = "DELETE FROM Bills WHERE ID = ?"
        cur.executemany(delete_command,param_values)

    all_credits = connn.execute("SELECT ID FROM VendorCredits;").fetchall()
    for i in range(0, len(all_credits), BATCH):
        sub_list = all_credits[i:i+BATCH]  # Create a sublist of 5 rows or the remaining rows
        param_values = [(item[0],) for item in sub_list]
        delete_command = "DELETE FROM VendorCredits WHERE ID = ?"
        cur.executemany(delete_command,param_values)
