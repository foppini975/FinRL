pkill -f coinbase_socket.py
git pull
rm -f nohup.out
nohup ./env_3.8/bin/python coinbase_socket.py &
tail -f nohup.out
