import sys, os, time, datetime, warnings, traceback
import requests
from progress.bar import IncrementalBar
import geopandas as gpd
import pandas as pd
from pandas.core.common import *
warnings.simplefilter(action="ignore", category= UserWarning)


banner = \
'''
/\__  _\                                                         
\/_/\ \/     ___ ___      __       __      __   _ __   __  __    
   \ \ \   /' __` __`\  /'__`\   /'_ `\  /'__`\/\`'__\/\ \/\ \   
    \_\ \__/\ \/\ \/\ \/\ \L\.\_/\ \L\ \/\  __/\ \ \/ \ \ \_\ \  
    /\_____\ \_\ \_\ \_\ \__/.\_\ \____ \ \____\\ \_\  \/`____ \ 
    \/_____/\/_/\/_/\/_/\/__/\/_/\/___L\ \/____/ \/_/   `/___/> \
                                   /\____/                 /\___/
                                   \_/__/                  \/__/ 
                 __                __            __               
 /'\_/`\        /\ \__            /\ \          /\ \__            
/\      \     __\ \ ,_\    __     \_\ \     __  \ \ ,_\    __     
\ \ \__\ \  /'__`\ \ \/  /'__`\   /'_` \  /'__`\ \ \ \/  /'__`\   
 \ \ \_/\ \/\  __/\ \ \_/\ \L\.\_/\ \L\ \/\ \L\.\_\ \ \_/\ \L\.\_ 
  \ \_\\ \_\ \____\\ \__\ \__/.\_\ \___,_\ \__/.\_\\ \__\ \__/.\_\
   \/_/ \/_/\/____/ \/__/\/__/\/_/\/__,_ /\/__/\/_/ \/__/\/__/\/_/
                                                   by Ilias Doukas
'''


'''
------------------------------------------------------------------------------
* ------------------------ G E T   M E T A D A T A ------------------------- *
------------------------------------------------------------------------------
'''
def get_metadata(polyDf, geoDfList, exportDir, bar):
    try:
        now = datetime.datetime.now().strftime("%H:%M:%S %d-%m-%Y")

        Xmin = polyDf['Xmin']
        Ymin = polyDf['Ymin']
        Xmax = polyDf['Xmax']
        Ymax = polyDf['Ymax']

        url = "https://services.arcgisonline.com/arcgis/rest/services/World_Imagery/MapServer/4/ \
            query?where=&text=&objectIds=&time=&geometry="+str(Xmin)+","+str(Ymin)+","+str(Xmax)+","+str(Ymax)+ \
            "&geometryType=esriGeometryEnvelope&inSR=4326&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true& \
            returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=4326&returnIdsOnly=false&returnCountOnly=false&orderByFields=& \
            groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=& \
            queryByDistance=&returnExtentsOnly=false&datumTransformation=&parameterValues=&rangeValues=&f=geojson"

        req = requests.get(url).json()
        if "features" in req:
            features = req['features']

            geoDf = gpd.GeoDataFrame.from_features(features).drop_duplicates()
            geoDfList.append(geoDf)
        else:
            with open(f"{exportDir}/errors.log", "a", encoding="utf-8") as file:
                file.write(f"✕ {now}: Error with polygon {polyDf['Polyname']}\n")

            df = pd.DataFrame([polyDf['Polyname'], polyDf['Xmin'], polyDf['Ymin'], polyDf['Xmax'], polyDf['Ymax']])
            dfT = df.T
            dfT.to_csv(f"{exportDir}/failedPolygons.csv", index= False, header= False, mode= "a")
        
        bar.next()
    
    except Exception as error:
        with open(f"{exportDir}/errors.log", "a", encoding="utf-8") as file:
            file.write(f"✕ {now}: Error with polygon {polyDf['Polyname']}: {traceback.format_exc()}\n")


'''
------------------------------------------------------------------------------
* ----------------- C R E A T E   M E T A D A T A   .S H P ----------------- *
------------------------------------------------------------------------------
'''
def export_shp(filename, exportDir, prefDate):
    try:
        now = datetime.datetime.now().strftime("%H-%M-%S_%d-%m-%Y")
        exportFname = f"{exportDir}/metadata_{now}.shp"
        exportFnamePrefshp = f"{exportDir}/prefMetadata_{now}.shp"
        exportFnameCsv = f"{exportDir}/polyDetails.csv"
        geoDfs = []

        try:
            if prefDate != "":
                prefDate = int(prefDate)
            else:
                prefDate = 0
        except:
            prefDate = 0

        polyDf = pd.read_csv(filename, delimiter= ",", names= ["Polyname", "Xmin", "Ymin", "Xmax", "Ymax"])

        bar = IncrementalBar('Processing', max= len(polyDf), suffix='%(percent)d%%')
        polyDf.apply(get_metadata, axis= 1, args= [geoDfs, exportDir, bar])
        bar.finish()
        
        finalGeoDf = gpd.GeoDataFrame(pd.concat(geoDfs, ignore_index=True), crs= "EPSG:4326").drop_duplicates()
        finalGeoDf.to_file(exportFname)

        finalGeoDf['SRC_DATE'] = finalGeoDf['SRC_DATE'].dropna().astype(int)
        prefGeoDf = finalGeoDf[(finalGeoDf['SRC_DATE'] > prefDate) & ((finalGeoDf['NICE_NAME'].str.lower() == "metro") | ("vivid" in finalGeoDf['NICE_NAME'].str.lower()))]
        if prefGeoDf: prefGeoDf.to_file(exportFnamePrefshp)

        detailsDf = pd.DataFrame([prefGeoDf['OBJECTID'], prefGeoDf['SRC_DATE'], prefGeoDf['SRC_RES'], prefGeoDf['MinMapLevel'], prefGeoDf['MaxMapLevel'], prefGeoDf['NICE_NAME']])
        detailsDfT = detailsDf.T
        detailsDfT.columns = ["ObjectId", "Date", "Resolution", "MinLevel", "MaxLevel", "Origin"]
        detailsDfT['Date'] = detailsDfT['Date'].apply(make_datetime)
        detailsDfT.to_csv(exportFnameCsv, index= False)

        return(True)

    except Exception as error:
        now = datetime.datetime.now().strftime("%H:%M:%S %d-%m-%Y")
        with open(f"{exportDir}/errors.log", "a", encoding="utf-8") as file:
            file.write(f"✕ {now}: Error: {traceback.format_exc()}\n")
        return(False)

'''
------------------------------------------------------------------------------
* -------------- F O R M A T   S O U R C E   D A T E T I M E --------------- *
------------------------------------------------------------------------------
'''
def make_datetime(date):
    try:
        year = str(int(date))[:4]
        month = str(int(date))[4:6]
        day = str(int(date))[6:]
        datetime = f"{year}/{month}/{day}"
        return(datetime)
    except:
        return(None)

'''
------------------------------------------------------------------------------
* ------------ C R E A T E   E X P O R T   D I R E C T O R Y --------------- *
------------------------------------------------------------------------------
'''
def create_export_dir():
    now = datetime.datetime.now().strftime("%d-%b-%Y_%H-%M-%S")

    if getattr(sys, "frozen", False):
        currentDirName = os.path.dirname(sys.executable).replace('\\', '/')
    elif __file__:
        currentDirName = os.path.dirname(__file__).replace('\\', '/')

    dir = f"{currentDirName}/Metadata_{now}"

    dirExists = os.path.isdir(dir)
    if not dirExists:
        os.mkdir(dir)

    return(dir)

'''
------------------------------------------------------------------------------
* ---------------- F O R M A T   P R O C E D U R E   T I M E --------------- *
------------------------------------------------------------------------------
'''
def formatTime(processTime):
    if processTime > 60:
        processTime //= 60
        if processTime > 60:
            processTime /= 60
            processTime = f"{round(processTime, 1)} hours"
        else:
            processTime = f"{processTime} minutes"
    else:
        processTime = f"{processTime} seconds"
    return(processTime)

'''
------------------------------------------------------------------------------
* ------------------------- M A I N   P R O G R A M ------------------------ *
------------------------------------------------------------------------------
'''
def main():
    try:
        print(banner)
        print("\n► Request satellite imagery metadata from ESRI...\n")
        exportDir = create_export_dir()

        if len(sys.argv) > 1:
            csvFname = str(sys.argv[1])

            if len(sys.argv) > 2:
                prefDate = str(sys.argv[2]).replace("-", "").replace("/", "")
                if len(prefDate) == 4:
                    prefDate += "0101"
                elif len(prefDate) == 6:
                    prefDate += "01"
                elif len(prefDate) != 8:
                    print("✕ Please input the right format of the date (e.g 2020-01-01).\n")
                    return
            else:
                prefDate = ""

            if csvFname:
                splittedFname = csvFname.split('/')
                fName = splittedFname[-1].split('.')
                fType = fName[-1]

                if fType == "csv":
                    start = time.time()
                    result = export_shp(csvFname, exportDir, prefDate)
                    end = time.time()

                    processTime = round(end - start, 1)
                    processTime = formatTime(processTime)

                    if result:
                        print(f"\n✓ Metadata reports have exported successfully after {processTime}\n")
                        errorExists = os.path.isfile(f"{exportDir}/errors.log")
                        if errorExists:
                            print(f"- For more details please open errors.log file.\n")
                    else:
                        print(f"\n✕ Failed to execute.\n- For more details please open errors.log file.")

                else:
                    print("✕ Please input the right name of csv file.\n")

            else:
                print("✕ Please input the right name of csv file and date as arguments.\n")
        else:
            print("✕ Please input the right name of csv file and date as arguments.\n")

        # Delete exported directory if is empty
        exportDirItems = os.listdir(exportDir)
        if not exportDirItems:
            os.rmdir(exportDir)
            
        os._exit(0)

    except Exception as error:
        now = datetime.datetime.now().strftime("%H:%M:%S %d-%m-%Y")
        print(f"\n✕ Failed to execute:\n{error}\n")
        print(f"- For more details please open errors.log file.\n")
        with open(f"{exportDir}/errors.log", "a", encoding="utf-8") as file:
            file.write(f"✕ {now}: {error}\n")
        os._exit(1)



if __name__ == "__main__":
    main()
