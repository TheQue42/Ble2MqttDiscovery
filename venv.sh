if [ -d py3 ]
then
    source py3/bin/activate	
else
    python3 -m venv py3
    source py3/bin/activate
fi
pip3 install -U pip 
pip3 install -U bleak asyncio Construct paho-mqtt

