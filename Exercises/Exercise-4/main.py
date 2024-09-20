import os
import json
import glob
import csv

#Recursive flattener
def flattener(target):
    header = []
    output = []
    if type(target) == list:
        for i in range(len(target)):
            (sh,so) = flattener(target[i])
            if len(sh) == 0:
                header.append(str(i))
            else:
                header += ['.'.join([str(i),x]) for x in sh]
            output += so
    elif type(target) == dict:
        for k,v in target.items():
            (sh,so) = flattener(v)
            #print(output)
            if len(sh) == 0:
                header.append(k)
            else:
                header += ['.'.join([k,x]) for x in sh]
            output += so
    else:
        output += [str(target)]
    return (header,output)

#New code using the suggested packages.
def main():
    x = os.getcwd()
    files = glob.glob('./**/*.json',recursive=True)
    for i in files:
        with open(i, 'r') as j:
            q = json.loads(j.read())
        (h,r) = flattener(q)
        d = dict(zip(h,r))
        with open(i.replace('.json','csv'),'w') as cf:
            writer = csv.DictWriter(cf,fieldnames=h)
            writer.writeheader()
            writer.writerow(d)
    print(glob.glob('./**/*.csv',recursive=True))
            

#Old code that did not use the suggested packages. (glob, csv)
def old_main():
    # your code here
    x = os.getcwd()
    cr = os.path.join(x,'data') #Lesson learned. Avoiding manipulating paths manually as much as possible. Slashes matter.
    files = []
    csvpaths = []
    for (dirpath, subdir, filelist) in os.walk(cr):
        for i in filelist:
            jsonpath = os.path.join(dirpath,i)
            try:
                with open(jsonpath, 'r') as j:
                    files.append(json.loads(j.read()))
                csvpaths.append(jsonpath.replace('.json','.csv'))
            except:
                pass
    for j in range(len(files)):
        (h,r) = flattener(files[j])
        c = '\n'.join([','.join(h),','.join(r)])
        with open(csvpaths[j], mode = 'a') as fn:
            fn.write(c)

if __name__ == "__main__":
    main()