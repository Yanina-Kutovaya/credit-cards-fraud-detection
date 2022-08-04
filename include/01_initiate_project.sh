kedro new
git init
git add .
git commit -m 'Initial commit'

git remote add origin git@github.com:Yanina-Kutovaya/credit-cards-fraud-detection.git 
git branch main
git checkout main
git merge master
git branch -D master

git push -uf origin main

pip install dvc[all]
dvc init
git commit -m "Initialize DVC"
dvc remote add -d s3store s3://credit-cards-data/dvc
dvc remote modify s3store endpointurl https://storage.yandexcloud.net

git rm -r --cached 'data'
git commit -m "stop tracking data"
git push

dvc add data 
git add data.dvc
dvc config core.autostage true
dvc install
git push

dvc add data        # after any chages in 'data' folder before commit
dvc pull            # after git pull to get changes from remote

dvc remove data.dvc # to stop data tracking