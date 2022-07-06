const hive = require('hive-driver');
const express = require ('express');
const bodyParser = require ('body-parser');
const Ably = require('ably');
let ably = new Ably.Realtime('jzdf0A.ouBMcw:J5th_O6E1nt5uCmwyn7pIIUzvgGuLStD1Gvs85FyOlM');
let channel = ably.channels.get('[product:ably-coindesk/crypto-pricing]btc:usd');

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

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

async function actual() {
    var result= 'hola';
    await channel.subscribe( function(message) {

        result =  message.data;
        channel.unsubscribe();
    });
    await sleep(4000);
    return result;
}
async function hola() {

    return client.connect(
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
        
        
        
        await session.close();
        console.log(utils.getResult(selectDataOperation).getValue());
        
        return utils.getResult(selectDataOperation).getValue(); 

    }).catch(error => {
        console.log(error);
    });

}

app.get('/prom', async (req,res) => {
    console.log(await actual());
    
    return res.status(200) .json( await hola());
 })
app.listen(PORT, () => {
    console.log("Servidor corriendo en el puerto", PORT )
    

    }
    );