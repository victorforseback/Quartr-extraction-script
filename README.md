# Quartr-extraction-script
A script that automatically pulls transcripts and presentationslides in text form from earnings calls of companies using Quartrs API.

Start manager.py to start extracting files.

manager.py has a list 'companies' at line 14. this line decides which companies you extract data from.

the files get_meta.py and meta_to_txt.py both use the import requests in order to keep your API key private.
You use it by writing this in the terminal:

Windows:
$env:QUARTR_API_KEY="YOUR_KEY"

MacOS & Linux:
export QUARTR_API_KEY="YOUR_KEY"
