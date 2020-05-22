import os
import json

def write_csv_file(fname, platform, features):
  f = open(os.path.join('/nfs/csb_data/csv', os.path.splitext(fname)[0] + '.csv'), 'w')
  f.write('LON,LAT,DEPTH,TIME,PLATFORM_NAME,PLATFORM_UUID\n')  
  for i in features:
    coords = i['geometry']['coordinates']
    props = i['properties']
    f.write('%s, %s, %s, "%s", "%s", "%s"\n' % (coords[0], coords[1], props['depth'], props['time'], platform['name'], platform['uuid']))

  f.close()



pointCount = 0
fileCount = 0
rootDir = './unpacked'
for dirName, suddirList, fileList in os.walk(rootDir):
  #print('directory: %s' % dirName)
  for fname in fileList:
    if (fname.endswith('.json')):
      fileCount = fileCount + 1
      #print('\t%s' % os.path.join(dirName, fname))
      with open(os.path.join(dirName, fname), 'r') as jsonFile:
        try:
          obj = json.load(jsonFile)
          platform = {
            'name': obj['properties']['platform']['name'],
            'uuid': obj['properties']['platform']['uniqueID']
          }

          numFeatures = len(obj['features'])
          pointCount = pointCount + numFeatures
          #write_csv_file(fname, platform, obj['features'])
          write_csv_file(fname, platform, obj['features'])
          print('%s: %s points' % (fname, numFeatures))
        except:
          print('ERROR: unable to read file %s' % os.path.join(dirName, fname))

print('%s files with a total of %s points' % (fileCount, pointCount))
