const hive = require('hive-driver');
const express = require ('express');
const bodyParser = require ('body-parser');
const PORT = process.env.PORT || 3050;
const app = express();
app.use(bodyParser.json());
const { TCLIService, TCLIService_types } = hive.thrift;
const client = new hive.HiveClient(
    TCLIService,
    TCLIService_types
);
const utils = new hive.HiveUtils(
    TCLIService_types
);
 
client.connect(
    {
        host: 'localhost',
        port: 25000
    },
    new hive.connections.TcpConnection(),
    new hive.auth.PlainTcpAuthentication()
    
).then(async client => {

    const session = await client.openSession({
        client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10
    });
    const selectDataOperation = await session.executeStatement(
        'SELECT AVG(VALOR) FROM BITCOIN '
    );
    await utils.waitUntilReady(selectDataOperation, false, () => {});
    await utils.fetchAll(selectDataOperation);
    await selectDataOperation.close();
    
    console.log(utils.getResult(selectDataOperation).getValue());
    
    await session.close();

    
}).catch(error => {
    console.log(error);
});

app.get('/prom', (req,res) => {
    const result = client.connect();
    console.log("hola:",result);
 })
app.listen(PORT, () => {
    console.log("Servidor corriendo en el puerto", PORT )
    

    }
    );