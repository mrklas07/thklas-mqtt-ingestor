require('dotenv').config();
const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');

// 1. Inicializar Supabase con privilegios de administrador
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

// 2. Conectar a HiveMQ (MQTT Seguro)
const mqttOptions = {
    username: process.env.MQTT_USER,
    password: process.env.MQTT_PASS,
    rejectUnauthorized: true // Requiere certificado válido del servidor
};

console.log('Conectando al Broker MQTT...');
const client = mqtt.connect(process.env.MQTT_HOST, mqttOptions);

client.on('connect', () => {
    console.log('✅ Conectado a HiveMQ Cloud');
    // Suscribirse a todos los tópicos de telemetría usando comodines (+)
    // Estructura esperada: telemetria/empresa_id/planta_id/gateway_id/generador_id
    client.subscribe('telemetria/+/+/+/+', (err) => {
        if (!err) {
            console.log('🎧 Escuchando telemetría de todos los generadores...');
        }
    });
});

client.on('message', async (topic, message) => {
    try {
        // Desglosar la ruta para saber de qué máquina viene el dato
        const route = topic.split('/');
        const generador_id = route[4];
        
        // Parsear los datos JSON enviados por el ESP32
        const datos = JSON.parse(message.toString());
        
        console.log(`📡 Recibido de Gen [${generador_id}]:`, datos);

        // -- PASO A: TRANSMISIÓN EN TIEMPO REAL --
        // Broadcast a la App Web/Móvil usando los canales de Supabase
        const channelName = `gen_vivo_${generador_id}`;
        supabase.channel(channelName).send({
            type: 'broadcast',
            event: 'actualizacion_datos',
            payload: datos
        });

        // -- PASO B: GUARDAR EN HISTÓRICO --
        // Lógica simple: Solo guardar si han pasado más de 60 seg desde el último guardado.
        // (En un entorno real de alto tráfico, usarías un sistema de caché en memoria como Redis aquí)
        if (deberiaGuardarEnBD(generador_id)) {
            const { error } = await supabase
                .from('telemetria_historica')
                .insert({
                    generador_id: generador_id,
                    timestamp: new Date().toISOString(),
                    datos: datos
                });

            if (error) console.error('❌ Error guardando en BD:', error.message);
            else console.log(`💾 Histórico guardado para Gen [${generador_id}]`);
        }

    } catch (error) {
        console.error('❌ Error procesando mensaje:', error);
    }
});

// Control simple de frecuencia de guardado (1 minuto por generador)
const ultimaVezGuardado = {};
function deberiaGuardarEnBD(generador_id) {
    const ahora = Date.now();
    const tiempoPasado = ultimaVezGuardado[generador_id] || 0;
    
    // 60,000 milisegundos = 1 Minuto
    if (ahora - tiempoPasado > 60000) {
        ultimaVezGuardado[generador_id] = ahora;
        return true;
    }
    return false;
}

// --- MINI SERVIDOR HTTP PARA RENDER ---
// Render requiere que un Web Service escuche en un puerto, de lo contrario apagará la app.
const http = require('http');
const port = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'text/plain');
    res.end('Ingestor MQTT de Thklas está activo y escuchando.\n');
});

server.listen(port, () => {
    console.log(`Servidor web de respaldo escuchando en el puerto ${port}`);
});