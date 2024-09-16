import { SQSClient, SendMessageCommand, ReceiveMessageCommand, DeleteMessageCommand, SQSClientConfig } from '@aws-sdk/client-sqs'
import dayjs from 'dayjs'

class SQSServiceTest {
    client: SQSClient
    startTime: dayjs.Dayjs
    counts: { [key: string]: number } = {}
    totalRequests: number = 0

    constructor() {
        const config: SQSClientConfig = {
            region: 'us-east-1',
            endpoint: 'http://localhost:4566',
            credentials: {
                accessKeyId: 'test',
                secretAccessKey: 'test'
            }
        }

        this.client = new SQSClient(config)
        this.startTime = dayjs()
    }

    async sendMessageToQueue(queueName: string, messageBody: string) {
        const params = {
            QueueUrl: `http://localhost:4566/000000000000/${queueName}`,
            MessageBody: JSON.stringify({ text: messageBody })
        }
        try {
            await this.client.send(new SendMessageCommand(params))
        } catch (error) {
            console.error(`Error sending message to ${queueName}:`, error)
        }
    }

    async receiveQueueMessage(queueName: string) {
        try {
            return this.client.send(
                new ReceiveMessageCommand({
                    QueueUrl: `http://localhost:4566/000000000000/${queueName}`,
                    AttributeNames: ['All'],
                    MaxNumberOfMessages: 1
                })
            )
        } catch (error) {
            console.error(error)
        }
    }

    async deleteQueueMessage(queueName: string, receiptHandle: string) {
        return this.client.send(
            new DeleteMessageCommand({
                QueueUrl: `http://localhost:4566/000000000000/${queueName}`,
                ReceiptHandle: receiptHandle
            })
        )
    }

    async sleep(ms: number) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async createQueueConsumer(queueName: string, fn: (message: any) => Promise<boolean>) {
        if (!this.counts[queueName]) {
            this.counts[queueName] = 0
        }

        while (true) {
            try {
                const result = await this.receiveQueueMessage(queueName)
                const message = result?.Messages && result?.Messages[0]

                const consume = async (message: any) => {
                    const result = await fn({ ...message, Body: message.Body });

                    if (!result) return false

                    await this.deleteQueueMessage(queueName, message?.ReceiptHandle)

                    this.counts[queueName] += 1
                    this.totalRequests += 1

                    const currentTime = dayjs()
                    if (currentTime.diff(this.startTime, 'minute') >= 1) {
                        Object.keys(this.counts).forEach((key) => {
                            console.log(`Queue: ${key} - Request call count per minute: ${this.counts[key]}`)
                            this.counts[key] = 0
                        })
                        console.log(`Total request call count per minute: ${this.totalRequests}`)

                        this.totalRequests = 0
                        this.startTime = currentTime
                    }
                }

                if (message) await consume(message)
            } catch (error) {
                console.error(`Error processing message from ${queueName}:`, error)
            }
        }
    }
}

async function runTest() {
    const sqsService = new SQSServiceTest()

    const processMessage = async (message: any) => {
        process.stdout.write(`\rProcessing message: ${message.Body}`);
        return true
    }

    const queueNames = ['test-queue-1', 'test-queue-2', 'test-queue-3']
    queueNames.forEach(async (queueName) => {
        for (let i = 0; i < 1000000; i++) {
            await sqsService.sendMessageToQueue(queueName, `Test message ${i}`)
        }
    })

    queueNames.forEach(async (queueName) => {
        sqsService.createQueueConsumer(queueName, processMessage)
    })
}

runTest().catch(console.error)