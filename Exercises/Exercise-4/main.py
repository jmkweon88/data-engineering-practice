import os
import json

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

def main():
    # your code here
    x = os.getcwd()
    x = x.replace(r'\Exercises\Exercise-4', r'') + r'\Exercises\Exercise-4'
    cr = x + r'\data'
    files = []
    csvpaths = []
    for (dirpath, subdir, filelist) in os.walk(cr):
        for i in filelist:
            jsonpath = dirpath + '\\' + i
            try:
                with open(jsonpath, 'r') as j:
                    files.append(json.loads(j.read()))
                csvpaths.append(jsonpath.replace(".json",".csv"))
            except:
                pass
    for j in range(len(files)):
        (h,r) = flattener(files[j])
        c = '\n'.join([','.join(h),','.join(r)])
        with open(csvpaths[j], mode = 'a') as fn:
            fn.write(c)

if __name__ == "__main__":
    main()