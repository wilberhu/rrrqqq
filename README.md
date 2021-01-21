# rrrqqqfront

'''
npm install
'''


# rrrqqqback


'''
pip install -r requirements.txt

ln -s rrrqqq/tushare_data rrrqqqback
'''
##sql中运行
'''
create database stock_api default character set utf8mb4 collate utf8mb4_unicode_ci;
'''

'''
python3 manage.py makemigrations companies compositions dailies datasets todays strategies

python3 manage.py migrate

python3 manage.py createsuperuser

jupyter-notebook -> tushare_data/sqlCtrl.ipynb

sh daily_shell.sh

'''


不更新指定文件
执行命令之前需要保持该文件为已同步状态 否则执行失败
'''
git update-index --assume-unchanged 文件名
'''
取消
'''
git update-index --no-assume-unchanged 文件名
'''


强制更新呢table
'''
python manage.py migrate --fake compositions zero
'''