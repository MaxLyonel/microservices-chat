import amqp, { Channel } from "amqplib";
import { v4 as uuidv4 } from "uuid";
import config from "../config/config";

class RabbitMQService {
    private requestQueue = "USER_DETAILS_REQUEST";
    private responseQueue = "USER_DETAILS_RESPONSE";
    private correlationMap = new Map();
    private channel!: Channel;

    constructor() {
        this.init();
    }

    async init() {
        const connection = await amqp.connect(config.msgBrokerURL!);
        this.channel = await connection.createChannel();
        await this.channel.assertQueue(this.requestQueue);
        await this.channel.assertQueue(this.responseQueue);

        this.channel.consume(
            this.responseQueue,
            (msg) => {
                if (msg) {
                    const correlationId = msg.properties.correlationId;
                    const user = JSON.parse(msg.content.toString());

                    // Cuando se recibe un mensaje en la cola de respuesta  se comprueba
                    // si hay un ID de correlacion
                    const callback = this.correlationMap.get(correlationId);
                    if (callback) { // si existe una funcion de callback asociada a ese ID de correlacion
                        callback(user); // se ejecuta
                        this.correlationMap.delete(correlationId); // se elimina del mapa de correlacion
                    }
                }
            },
            { noAck: true }
        );
    }

    // se utiliza para enviar una Solicitud de Detalles de Usuario a la cola 'USER_DETAILS_REQUEST'
    // Cuando se recibe una respuesta a la solicitud de detalles de usuario en la cola 'USER_DETAILS_REQUEST'
    // se activa el callback asociado a traves de ID de correlacion
    async requestUserDetails(userId: string, callback: Function) {
        const correlationId = uuidv4();
        this.correlationMap.set(correlationId, callback);
        this.channel.sendToQueue( // enviar a la cola
            this.requestQueue, // 'user_details_request'
            Buffer.from(JSON.stringify({ userId })),
            { correlationId }
        );
    }

    // este metodo es el encargado de enviar un mensaje al receptor identificado por ID
    async notifyReceiver(
        receiverId: string,
        messageContent: string,
        senderEmail: string,
        senderName: string
    ) {
        // el servicio obtiene los detalles del usuario receptor mediante una solicitud a la cola 'USER_DETAIL_REQUEST'
        await this.requestUserDetails(receiverId, async (user: any) => {
            const notificationPayload = {
                type: "MESSAGE_RECEIVED",
                userId: receiverId,
                userEmail: user.email,
                message: messageContent,
                from: senderEmail,
                fromName: senderName,
            };

            try {
                // Una vez que se obtienen los detalles del usuario receptor, se construye un objeto de notificación y se
                // envía a otra cola para ser procesado por otros servicios.
                await this.channel.assertQueue(config.queue.notifications);
                this.channel.sendToQueue(
                    config.queue.notifications,
                    Buffer.from(JSON.stringify(notificationPayload))
                );
            } catch (error) {
                console.error(error);
            }
        });
    }
}

export const rabbitMQService = new RabbitMQService();
