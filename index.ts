import { SQSClient, SendMessageCommand, ReceiveMessageCommand, DeleteMessageBatchCommand, SQSClientConfig, Message } from '@aws-sdk/client-sqs'
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

    async receiveQueueMessages(queueName: string, batchSize: number = 10) {
        try {
            return this.client.send(
                new ReceiveMessageCommand({
                    QueueUrl: `http://localhost:4566/000000000000/${queueName}`,
                    AttributeNames: ['All'],
                    MaxNumberOfMessages: batchSize,
                    WaitTimeSeconds: 0
                })
            )
        } catch (error) {
            console.error(error)
        }
    }

    async deleteQueueMessages(queueName: string, receiptHandles: string[]) {
        const entries = receiptHandles.map((receiptHandle, index) => ({
            Id: index.toString(),
            ReceiptHandle: receiptHandle
        }))
        return this.client.send(
            new DeleteMessageBatchCommand({
                QueueUrl: `http://localhost:4566/000000000000/${queueName}`,
                Entries: entries
            })
        )
    }

    async processMessages(queueName: string, fn: (message: any) => Promise<boolean>, batchSize: number = 10) {
        const result = await this.receiveQueueMessages(queueName, batchSize)
        const messages = result?.Messages || []
        if (messages.length > 0) {
            const receiptHandles: any[] = []
            const processingPromises = messages.map(async (message) => {
                const success = await fn(message)
                if (success) {
                    receiptHandles.push(message.ReceiptHandle)
                }
            })

            await Promise.all(processingPromises)

            if (receiptHandles.length > 0) {
                await this.deleteQueueMessages(queueName, receiptHandles)
            }

            this.counts[queueName] = (this.counts[queueName] || 0) + messages.length
            this.totalRequests += messages.length

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
    }

    createQueueConsumer(queueName: string, fn: (message: any) => Promise<boolean>, batchSize: number = 10, maxParallel: number = 50) {
        const consumers = new Array(maxParallel).fill(null).map(async () => {
            while (true) {
                await this.processMessages(queueName, fn, batchSize)
            }
        })
        Promise.all(consumers).catch(console.error)
    }
}

async function runTest() {
    const sqsService = new SQSServiceTest()

    const processMessage = async (message: Message) => {
        process.stdout.write(`\rProcessing message: ${message.Body}`);
        return true
    }

    const queueNames = ['test-queue-1', 'test-queue-2', 'test-queue-3']

    queueNames.forEach(async (queueName) => {
        for (let i = 0; i < 1000000; i++) {
            await sqsService.sendMessageToQueue(queueName, `Test message ${i}`)
        }
    })

    queueNames.forEach(queueName => {
        sqsService.createQueueConsumer(queueName, processMessage, 10, 50)
    })
}

runTest().catch(console.error)
