# Last Modified By: Githika Tondapu

import datetime
import time
import sys
import os, shutil
import zipfile
import ftplib
from PIL import Image
import pickle
import boto
import boto.s3
from boto.s3.key import Key
import extract_utilities_ftp
import logging
lastFileProcessed = ""
fallbackFile = ""

# Initial Processing of the files.
def initProcessing(filesToProcess):
    filename = "Starting..."
    mergeList = []
    lastFileProcessed = ""
    for f2p in filesToProcess:
        if(filename in f2p):
            mergeList.append(f2p)
        else:
            if not mergeList:
                mergeList.append(f2p)
                filename = f2p[0:8]
            else:
                if(len(mergeList) > 1):
                    lastFileProcessed = CropAndMergeFiles(mergeList, True)
                else:
                    lastFileProcessed = CropAndMergeFiles(mergeList, False)
                mergeList = []
                mergeList.append(f2p)
                filename = f2p[0:8]
    if(len(mergeList) > 1):
        lastFileProcessed = CropAndMergeFiles(mergeList, True)
    else:
        lastFileProcessed =  CropAndMergeFiles(mergeList, False)
    return lastFileProcessed

# Blend foreground, background images and save the image to E:\TEMP\OP_Templates\
def createBaseComposite(topimage, OutputName):
    folderPath = myConfig['template_Folder']
    background = Image.open(folderPath + "Baselayer_whiteOutline.png", "r")
    foreground = topimage
    rgbabackground = background.convert("RGBA")
    rgbaforeground = foreground.convert("RGBA")
    theComposit = Image.alpha_composite(rgbabackground, rgbaforeground)
    datelessName = OutputName[:-10]
    legend = Image.open(folderPath + datelessName + "vertical_Colorscale.png", "r")
    theComposit.paste(legend, (1800,50))
    theComposit.save(myConfig['extract_Folder'] + OutputName +"_landmask.png")
# Merge and Crop the image to the box
def CropAndMergeFiles(files, merge):
    folderPath = myConfig['extract_Folder']
    year = files[0][1:5] 
    juliandate = files[0][5:8]
    lastFileProcessed = files[0][0:8]
    date = datetime.datetime(int(year), 1, 1) + datetime.timedelta(int(juliandate) -1)
    for f2p in files:
        downloadedFileLocation = folderPath + f2p
        with open(downloadedFileLocation, "wb") as f:
            ftp.retrbinary("RETR %s" % f2p, f.write)
    theComposit = None
    if(merge == True):
        background = Image.open(folderPath + files[0], "r")
        foreground = Image.open(folderPath + files[1], "r")
        rgbabackground = background.convert("RGBA")
        rgbaforeground = foreground.convert("RGBA")
        theComposit = Image.alpha_composite(rgbabackground, rgbaforeground)
    else:
        nonTransparent = Image.open(folderPath + f2p, "r")
        theComposit = nonTransparent.convert("RGBA")

    datas = theComposit.getdata()

    newData = []
    for item in datas:
        if item[0] == 0 and item[1] == 0 and item[2] == 0:
            newData.append((255, 255, 255, 0))
        else:
            newData.append(item)

    theComposit.putdata(newData)
    box = (82, 51, 82 + 1900, 51 + 1900)
    output_img = theComposit.crop(box)
    OutputName = "MODIS-Aqua-CHLOR_A-yyyy.mm.dd"
    if('.sst' in files[0]):
        OutputName = "MODIS-Aqua-SST-" + date.strftime('%Y.%m.%d')
    elif('.chlor_a' in files[0]):
        OutputName = "MODIS-Aqua-CHLOR_A-" + date.strftime('%Y.%m.%d')
    elif('.nflh' in files[0]):
       OutputName = "MODIS-Aqua-NFLH-"  + date.strftime('%Y.%m.%d')
    output_img.save(folderPath + OutputName +".png")
    logging.info('Processing: %s' % OutputName)
    createBaseComposite(output_img, OutputName)

    kmlPrefix = ""
    if('.sst' in files[0]):
        kmlPrefix = "MODIS-Aqua-SST-"
    elif('.chlor_a' in files[0]):
        kmlPrefix = "MODIS-Aqua-CHLOR_A-"
    elif('.nflh' in files[0]):
       kmlPrefix = "MODIS-Aqua-NFLH-" 
    with open(folderPath + kmlPrefix + date.strftime('%Y.%m.%d') + ".kml", "w+") as fout:
        with open(myConfig['template_Folder'] + kmlPrefix + "yyyy.mm.dd.kml", "rt") as fin:
            for line in fin:
                fout.write(line.replace('yyyy.mm.dd', date.strftime('%Y.%m.%d')).replace('SourcePNGFileName', kmlPrefix + date.strftime('%Y.%m.%d') + ".png"))
    packageKMZfiles(folderPath + kmlPrefix, date.strftime('%Y.%m.%d'), folderPath, kmlPrefix)
    return lastFileProcessed
 
# Zip and Upload the files to S3 bucket 
def packageKMZfiles(filename, theDate, folderPath, kmlPrefix):
    pack = True
    with zipfile.ZipFile(filename + theDate + '.kmz', 'w') as myzip:
        myzip.write(filename + theDate + '.png', os.path.relpath(filename + theDate + '.png', folderPath))  #name-date.png
        myzip.write(filename + theDate + '.kml', os.path.relpath(filename + theDate + '.kml', folderPath))  #name-date.kml
        myzip.write(myConfig['template_Folder'] + kmlPrefix +'vertical_Colorscale.png', os.path.relpath(filename +'vertical_Colorscale.png', folderPath))  #name-vertical_Colorscale.png
    f = open( 'lastProcessed.dat', 'w' )
    uploadToS3(filename + theDate + '.kmz', True, kmlPrefix + theDate + '.kmz')
    uploadToS3(filename + theDate + '_landmask.png', False, kmlPrefix + theDate + '_landmask.png')

# Uploading the files to S3 bucket using the credentials AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
def uploadToS3(filename, isKmz, keyName):
    logging.info('Uploading: %s to S3 Bucket' % filename)
    conn = boto.connect_s3(myConfig['AWS_ACCESS_KEY_ID'], myConfig['AWS_SECRET_ACCESS_KEY'],is_secure=False)
    b = conn.get_bucket(myConfig['bucket'])
    conn.suppress_consec_slashes = False
    key_name = keyName
    path = myConfig['bucket_path']
    full_key_name = os.path.join(path, key_name)
    k = Key(b)
    k.key = full_key_name
    if(isKmz):
        k.set_metadata('Content-Type', 'application/vnd.google-earth.kmz' )
    k.set_contents_from_filename(filename)
    k.set_acl('public-read') # Make the file public
	
# Clean the extract folder E:\Temp\OP_Extract\
def cleanExtractFolder():
    folder = myConfig['extract_Folder']
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception, e:
            print e
			
#************************************************BEGIN PROCESS****************************************

pkl_file = open('config.pkl', 'rb')
myConfig = pickle.load(pkl_file)
pkl_file.close()

logDir = myConfig['logFileDir']
logging.basicConfig(filename=logDir+ '\OP_log_'+datetime.date.today().strftime('%Y-%m-%d')+'.log',level=logging.INFO, format='%(asctime)s: %(levelname)s --- %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.info('OP ETL started')
ftp = None
try:
    ftp = ftplib.FTP(myConfig['ftp_server'])
    ftp.login()
    ftp.cwd(myConfig['ftp_path'])
except Exception, e:
    logging.error('Error occured in connecting to ftp, %s' % e)
if ftp == None:
    logging.error('Nothing will be processed due to ftp failure')
else:
    files = extract_utilities_ftp.listFtpDirectory(ftp)
    files.sort()
    afterLast = False
    sstToProcess = []
    chlorToProcess = []
    nflhToProcess = []
    stuffToProcess = False
    with open('lastProcessed.dat', 'r') as f:
        first_line = f.readline()
    theFileWeLastProcessed = first_line
    logging.info('Last file Processed: %s' % theFileWeLastProcessed)
    #    A2015276    used for testing
    #    A2013270    to get first one on the ftp	A2013304 first new one
    for f in files:
        if(theFileWeLastProcessed in f):
            afterLast = True
        if(afterLast == True):
            if(theFileWeLastProcessed not in f and '.nc' not in f and '.TC.png' not in f and '.aot_869' not in f):
                stuffToProcess = True
                if('.sst' in f):
                    sstToProcess.append(f)
                elif('.chlor_a' in f):
                    chlorToProcess.append(f)
                elif('.nflh' in f):
                    nflhToProcess.append(f)
    if(stuffToProcess == True):
        if sstToProcess:
            lastFileProcessed = initProcessing(sstToProcess)
        else:
            logging.info('Notice: there were no SST files to process')
        if chlorToProcess:
            lastFileProcessed = initProcessing(chlorToProcess)
        else:
            logging.info('Notice: there were no chlor-a files to process')
        if nflhToProcess:
            lastFileProcessed = initProcessing(nflhToProcess)
        else:
            logging.info('Notice: there were no NFLH files to process')

        year = lastFileProcessed[1:5]
        juliandate = lastFileProcessed[5:8]
    
        date = datetime.datetime(int(year), 1, 1) + datetime.timedelta(int(juliandate) -1)
        ds = date.strftime('%Y.%m.%d')
        folderPath = myConfig['extract_Folder']
        logging.info('Creating latest and landmask')
        try:
            if sstToProcess:
                uploadToS3(folderPath + "MODIS-Aqua-SST-" + ds + ".kmz", True, 'MODIS-Aqua-SST-Latest.kmz')
                uploadToS3(folderPath + "MODIS-Aqua-SST-" + ds + "_landmask.png", False, 'MODIS-Aqua-SST-Latest_landmask.png')
            else:
                logging.info('Notice: there were no updates to the SST latest files to process')
        except Exception, e:
            logging.error('Error occured uploading MODIS-Aqua-SST-%s.kmz", %s' % (ds, e))
        try:
            uploadToS3(folderPath + "MODIS-Aqua-CHLOR_A-" + ds + ".kmz", True, 'MODIS-Aqua-CHLOR_A-Latest.kmz')
            uploadToS3(folderPath + "MODIS-Aqua-CHLOR_A-" + ds + "_landmask.png", False, 'MODIS-Aqua-CHLOR_A-Latest_landmask.png')
        except Exception, e:
            logging.error('Error occured uploading MODIS-Aqua-CHLOR_A-%s.kmz", %s' % (ds, e))
        try:
            uploadToS3(folderPath + "MODIS-Aqua-NFLH-" + ds + ".kmz", True, 'MODIS-Aqua-NFLH-Latest.kmz')
            uploadToS3(folderPath + "MODIS-Aqua-NFLH-" + ds + "_landmask.png", False, 'MODIS-Aqua-NFLH-Latest_landmask.png')
        except Exception, e:
            logging.error('Error occured uploading MODIS-Aqua-NFLH-%s.kmz", %s' % (ds, e))
        if not lastFileProcessed:
            logging.info('Last processed not updated')
        else:
            f = open( 'lastProcessed.dat', 'w' )
            f.write(lastFileProcessed) 
            f.close()
        cleanExtractFolder()
    else:
        logging.info('There was nothing to process')
logging.info('******************************OP ETL finished******************************')