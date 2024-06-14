It's only work using .csv extensions files

Open in cmd with: python bulk_insert.py

Edit the config.json and change the variables to do the actions.

Don't forget to check the link of <a src="https://stackoverflow.com/a/27516892">escape characters</a> if you need

All paths needs to have double inverted-dash and the Full Path, not Relative Path. e.g.: C:\\username\\dir\\archive.csv

And most importantly, install <a src="https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server">ODBC driver</a>. This script is using ODBC 18