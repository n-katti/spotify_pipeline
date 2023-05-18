# Merge changes of new branch with main branch
1. Add and commit changes
2. Run the following command: git push --set-upstream origin <branch>
3. Click on the link in the terminal and approve commit

OR 

git checkout master
git merge myBranch
git push

# How to pull new changes back after merging changes
1. Switch back to main branch: git checkout main 
2. Pull new changes: git pull
