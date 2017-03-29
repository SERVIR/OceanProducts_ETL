Introduction: Custom ETL script to convert SeaWiFS Data Analysis System (SeaDAS), an image processing and analysis software package for ocean data, and a subscription service to acquire the highest resolution ocean data possible (1 km) from MODIS data into useful images that can be overlaid on maps and integrated with other geospatial content daily for immediate use.

Instructions to schedule the task on a machine:
1. Go to OceanPickle.py and enter your paths and credentials
2. Save and run OceanPickle.py. This should generate config.pkl file in your folder
3. Search for Administrative tools in your machine
4. Go to Task Scheduler
5. Click on "Create Task" in the right pane and enter details 
6. Schedule the task in Triggers tab
7. Upload the OceanProductsETL.bat file in the Actions tab
