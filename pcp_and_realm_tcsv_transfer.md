## Project
- Python 3.12, uv venv, PyCharm
- this is a utily script that will make importing and exporting between Planning Center People and Realm easier.
- the inputs will be an export file from either package and a translate file that will identify which fields to 
  keep and the new name if it is to be renamed. 

## Libraries
- utility functions are contained in this directory but i will specify particualr ones to use with thier locations /Users/Denise/Library/CloudStorage/Dropbox/PythonPrograms/uvbekutils/uvbekutils
- for picking a file: /Users/Denise/Library/CloudStorage/Dropbox/PythonPrograms/uvbekutils/uvbekutils/select_file.py
- for verifying file format/columns: /Users/Denise/Library/CloudStorage/Dropbox/PythonPrograms/uvbekutils/uvbekutils/standardize_columns.py
- for exiting with a message or promting to exit or not use functions exit_yes and exit_yes_no contained in 
  /Users/Denise/Library/CloudStorage/Dropbox/PythonPrograms/uvbekutils/uvbekutils/bek_funcs.py
- - Before writing new utility code, ask if i have one for the function
- check libs/README.md for existing functions
- libs/ contains reusable modules — read before reimplementing

## Data Layout
- a file that originated as an export from pcp is: /Users/Denise/Downloads/fourth-universalist-society-export-1782870.csv
- i dont yet have file that originated as an export from realm but we can fill that in later: I have the header 
  values
- a file used for identifying the fields to be kept and mapping them to new names is: 
  /Users/Denise/Library/CloudStorage/Dropbox/4thU/process_automations/pcp_root/pcp/pcp_realm_transfer_column_map.
  xlsx. there should be tab named 'columns' or error.

## The script flow
- prompt for which direction the import export is going: CPC to realm or realm to PCP. the file that was exported 
  wnd reformated will be refered to as the origin file.  the fields on that file will be refered to as the origin 
  field names. possibly extraneuos fields will present in the mapping file; the ones required will be dependant on a 
  keep field on the column referring to the origin file. 
- files will read into dataframes for processing.
- the output will be produced in a dataframe then be written out to a csv file 
- prompt for the origin data file, the one containing the data that was exported from the originating system. use 
  select_file function for this.
- check a specified list of data fields to ensure the selected file is the correct format. do this by using 
  standardize_columns.py.  can specify the list of fields as a contastant at the top of the script, one constant for 
  pcp fields and one for realm fields.  these should be checked as a subset of the input since some columns may be 
  removed to speed and make the process more accurate.
- check the sheet named 'columns' for specific required columns in the map spreadsheet in the same way.  
- fields other than those specified in the keep column associated with the origin file will be ignored. if keep is 
  an x then the field will keep the same name on the output file.

## checking the file formats
- map sheet should have these fields, others can be ignored: pcp_column_name, pcp_keep, realm_column_name, realm_keep
- pcp origin sheet should have these fields, others can be ignored: First Name, Last Name, Home Email, Work Email
- realm origin sheet should have these fields, others can be ignored: First Name, Middle Name, Last Name, Primary 
  Email, Alternate Email

## data integrity checks
- any field identified as a keep in the origin file must be on the input file
- before writing the final output file, i want to browse the dataframe with the final field names that will be written

## Data Handling Rules
- Only read individual files i specify and not contents of directories
- Fields are case sensitive on import and export so 
  should not be changed
- Before searching for files ask me if i can tell you where they are located to save tokens
- custom fields in PCP export files contain prefixes representing their screen name (e.g., "master::"). Strip the 
  screen names along with the '::' from column headers on import
- for browsing the dataframe try dtale - heres some sample code
- '''import dtale

def browse(df):
    dtale.show(df, open_browser=True)
    input("Press Enter to continue...")  # keeps it alive

# then in your script:
df = pd.read_csv("data/input/members.csv")
browse(df)'''

## Conventions
- Type hints on all functions
- Use existing libs/ functions before writing new ones
- Keep functions small and testable

## Testing
- Run: uv run pytest tests/

## Token Discipline
- Be concise — no preamble, no restating the question
- Don't read files unless needed for the current task
- When exploring code, use subagents

## Compaction Rules
- When compacting, preserve: list of modified files, current task status, any failing tests
