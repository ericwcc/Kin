# Kin
A generic (K)afka (in)gestion agent

## Usage
```shell
python src/kin.py publish AUTO_Flow_Pressure --topic 1 --bootstrap-servers 1 --filter "column:0>=2024-05-10 00:00:00" --filter "column:0<=2024-05-15 03:00:00" --no-header --mapper "filename=NodeId"  --mapper "column:0=Datetime"
```