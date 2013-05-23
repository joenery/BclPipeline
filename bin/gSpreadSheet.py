#!/usr/bin/env python
# Standard Python Modules
import os
import sys
import json
import readline
import glob

# Module from Repo
from gidConversions import GIDConversion 

# Google Modules
try:
    import httplib2
    from httplib2 import Http
    from apiclient import errors
    from apiclient.discovery import build
    from oauth2client.client import flow_from_clientsecrets
    from oauth2client.file import Storage
except ImportError:
    print("Couldn't find one of the following: httplib2,apiclient,oauth2client")
    print("Do: pip install --upgrade google-api-python-client")
    sys.exit(1)


class tabCompleter(object):
    """
    A tab completer that can either complete from
    the filesystem or from a list.
    
    Partially taken from:
    http://stackoverflow.com/questions/5637124/tab-completion-in-pythons-raw-input
    """
    def __init__(self):
        """
        Setup readline for tab completions
        """
        readline.set_completer_delims('\t')
        readline.parse_and_bind("tab: complete")

    def pathCompleter(self,text,state):
        """
        Simple Module for Tab Completing
        """
        line   = readline.get_line_buffer().split()

        return [x for x in glob.glob(text+'*')][state]

    
    def createListCompleter(self,l):
        """
        This is a closure that creates a method that autocompletes from
        the given list.
        """
        def listCompleter(text,state):
            line   = readline.get_line_buffer()

            if not line:
                return [c for c in l][state]

            else:
                return [c for c in l if c.startswith(line)][state]

        self.listCompleter = listCompleter


class authorizer(object):
    """
    The authorizer handles all of the oauth2
    verifications and checking to make sure there is
    a json blurb with client secrets in the script's
    running directory.
    
    Returns: oauth2clint.file Storage object with credentials
    """
    def __init__(self):
        # Script Location
        script = os.path.realpath(sys.argv[0])
        script_location = os.path.split(script)[0]
        
        self.client_secrets_json = os.path.join(script_location,"client_secrets.json")

        if not os.path.isfile(self.client_secrets_json):
            print("Couldn't find client_screts.json in %s" % script_location)
            print("Make sure to download it from your Google API console and place it in your scripts directory")
            sys.exit(1)

    def authorize(self):
        # Step 0 Create Flow
        flow = flow_from_clientsecrets(self.client_secrets_json,
                                       scope=['https://spreadsheets.google.com/feeds',
                                              'https://www.googleapis.com/auth/drive'],
                                       redirect_uri="urn:ietf:wg:oauth:2.0:oob")

        # Step 1 Get an authorize URL and get Code
        authorize_url = flow.step1_get_authorize_url()
        
        print 'Copy and paste the following link in your browser: \n\n%s' % (authorize_url)
        code = raw_input("\n\nEnter verification code: ").strip()
        
        # Step 2: Exchange Code for credentials
        credentials = flow.step2_exchange(code)
        
        # Store credentials
        storage = Storage('credentials')
        storage.put(credentials)

        return storage

def retrieve_all_files(service):
    """Retrieve a list of File resources.
    
    Args:
      service: Drive API service instance.
    Returns:
      List of File resources.
    
    Taken from google!
    """
    result = []
    page_token = None
    while True:
        try:
            param = {}
            if page_token:
                param['pageToken'] = page_token
            files = service.files().list(**param).execute()
    
            result.extend(files['items'])
            page_token = files.get('nextPageToken')
            if not page_token:
                break
        except errors.HttpError, error:
            print 'An error occurred: %s' % error
            break
    return result

def get_user_spreadsheets(drive):
    user_files = retrieve_all_files(drive)

    spreadsheets = {}

    for file in user_files:
        file_type = file["mimeType"]
        file_name = file["title"]

        try:
            export_links = file["exportLinks"]
        except KeyError:
            export_links = None
        
        try:
            explicitly_trashed = file["explicitlyTrashed"]
        except KeyError:
            explicitly_trashed = False
        
        try:
            id = file["id"]
        except KeyError:
            id = None

        if file_type == "application/vnd.google-apps.spreadsheet" and not explicitly_trashed:
            spreadsheets[file_name] = {"export_links":export_links,"id":id}

    return spreadsheets
    
def getSpreadsheetCSV():
    """
    This is meant to be an importable module that will handle
    all of the major steps that are needed in order to get a CSV from
    Google Drive.

    Returns: an array where every line is an array and every item is a column

    eg: "1,2,3,4\n5,6,7,8" would be returned as
        [["1","2","3","4"],["5","6","7","8"]]

    This would enable you to convert the sample sheet or perform operations on it
    before writing it out to disk
    """
    a = authorizer()
    storage = a.authorize() 
     
    # Authorize an http object with the credentials
    # Use that to build the drive service
    http = storage.get().authorize(Http())
    drive = build('drive','v2',http=http)

    # Get List of Spreadsheet Files in Drive
    spreadsheets_info  = get_user_spreadsheets(drive)
    spreadsheets_title = [x.encode('ascii','ignore') for x in spreadsheets_info]
    spreadsheets_title.sort()

    print
    print("Here's a list of spreadsheets in your Google Drive.")
    print("Please choose one to export:\n")
    print("   ".join(spreadsheets_title))
    print

    # Initialize tab completer
    t = tabCompleter()
    t.createListCompleter(spreadsheets_title)
    readline.set_completer_delims('\t')
    readline.parse_and_bind("tab: complete")
    readline.set_completer(t.listCompleter)

    # Get spreadsheet title 
    # Then from that get spreadsheet ID
    spreadsheet_title = raw_input("What spreadsheet do you want to export? ").strip("\t").strip("\n").strip("   ") 
    spreadsheet_info  = spreadsheets_info[spreadsheet_title]
    spreadsheet_id    = spreadsheet_info['id'].encode('ascii','ignore')

    # Make a request for the Worksheets in the spreadsheet
    headers = {"GData-Version":"3.0"}
    request_url = "https://spreadsheets.google.com/feeds/worksheets/%s/private/full?alt=json" % (spreadsheet_id)
    http = storage.get().authorize(Http())
    response,content = http.request(request_url,
                                   method="GET",
                                   body=None,
                                   headers=headers)
    
    if not response['status'] == "200":
        print "The server was all like, 'Fuck you, man'"
        print response
        sys.exit(1)

    content = json.loads(content)
    feed = content['feed']
    worksheets = {}

    for worksheet in feed['entry']:
        worksheets[worksheet['title']['$t']] = worksheet

    worksheet_titles = [x.encode('ascii','ignore') for x in worksheets]
    t.createListCompleter(worksheet_titles)
    readline.set_completer(t.listCompleter)

    if len(worksheet_titles) > 1:
        print
        print("Here's a list of worksheets in %s" %  spreadsheet_title)
        print("Please choose one to export:\n")
        print("   ".join(worksheet_titles))
        print 
        worksheet = raw_input("Which worksheet would you like to export? ").strip("\t").strip("\n")
    else:
        worksheet = worksheet_titles[0]

    # Get Worksheet GID and make request to google for download
    # Google doesn't actually give you the real GID. They give a funny formatted
    # one that you have to change into a GID yourself.
    print
    print("Requesting %s from %s from Google" % (worksheet,spreadsheet_title))
    
    google_worksheet_id =  worksheets[worksheet]['id']['$t'][-3:]
    worksheet_gid = GIDConversion.conversionChart(google_worksheet_id)

    export_csv_url = "https://docs.google.com/feeds/download/spreadsheets/Export?key=%s&exportFormat=csv&gid=%s" % (spreadsheet_id,worksheet_gid)
    
    # Make the authorized request
    http = storage.get().authorize(Http())
    response,content = http.request(export_csv_url,
                                    headers=headers)

    if not response['status'] == "200":
        print "The sever is piiiiiiised at you!"
        sys.exit(1)
    
    return [x.split(",") for x in content.split("\n")]


if __name__=="__main__":
    
    print("\nWelcome to gSpreadSheet 1.0\n")
    print("First: I need to log you into your Google Drive\n")
    
    csv = getSpreadsheetCSV()

    # Where does the user want to write this file to?
    print
    t = tabCompleter()
    readline.set_completer(t.pathCompleter)

    while True:
        destination_folder = raw_input("What folder would you like to save this csv in: ")
    
        if destination_folder == "./" or not destination_folder:
            destination_folder = os.getcwd()
    
        else:
            destination_folder = os.path.realpath(destination_folder)

        if not os.path.isdir(destination_folder):
            print
            print("That's not a real path!\nTryAgain!\n")
        else:
            break
    
    file_name = raw_input("What would you like to call this csv: ")

    print("Saving CSV to disk")

    with open(os.path.join(destination_folder,file_name),"w") as csv_output:
        csv = [",".join(line) for line in csv]
        csv_string = "\n".join(csv)
        csv_output.write(csv_string)
    
    print("Done!")
    print
    print("\t\tPath to your CSV %s\t" % os.path.join(destination_folder,file_name))
    print 
