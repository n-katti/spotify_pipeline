import os
import shutil

path = r'C:\Users\nikhi\OneDrive\Documents\Personal\Recruiting\2023'

w = os.walk(path)
listt = []
for root, dirs, files in w:
    root = root.split('\\')
    root[0] = 'C:\\'
    # print(root)
    root = os.path.join(*root[:9])
    # print(root)
    listt.append(root)
    
listt = set(listt)

listt = list(listt)
print(listt)

for x in listt:
    if 'Givebutter' not in x and 'Archived' not in x and 'Square' not in x:
        try:
            shutil.copy(r"C:\Users\nikhi\OneDrive\Documents\Personal\Recruiting\2023\2023 Nikhil Katti Resume_Full.docx", x)
            shutil.copy(r"C:\Users\nikhi\OneDrive\Documents\Personal\Recruiting\2023\2023 Nikhil Katti Resume_Full.pdf", x)
            print(x)
        except:
            print(f"Failed for {x}")

# os.chdir(current_dir)

# os.getcwd()

