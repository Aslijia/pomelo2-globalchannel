import { promisify } from 'util'

const creater = require('../components/globalChannel')

jest.mock('pomelo2-logger', () => {
    return {
        getLogger() {
            return {
                trace: console.log,
                debug: console.log,
                warn: console.log,
                error: console.error
            }
        }
    }
})
describe('unit test:', () => {
    const app = {
        set: () => {},

        rpcInvoke: (id: string, opts: any) => {
            console.log('rpcInvoke', id, opts)
        },

        getServersByType: () => {
            return [
                { id: 'connector-server-1' },
                { id: 'connector-server-2' },
                { id: 'connector-server-3' },
                { id: 'connector-server-4' },
                { id: 'connector-server-5' }
            ]
        }
    }

    const services = creater(app, {
        url: 'redis://127.0.0.1:6379',
        cleanOnStartUp: true,
        options: {
            db: 6,
            keyPrefix: 'keyPrefix#'
        }
    })

    test('start service', async () => {
        await promisify((services as any).start.bind(services))()
    })
    test('add', async () => {
        await services.add('global-chat', '10010', 'connector-server-1')
        await services.add('global-chat2', '10010', 'connector-server-1')
        await services.add('global-chat', '10011', 'connector-server-1')
        await services.add('global-chat', '10012', 'connector-server-2')
        await services.add('global-chat2', '10013', 'connector-server-3')
        await services.add('global-chat', '10014', 'connector-server-5')
        await services.add('global-chat', 10015, 'connector-server-5')

        expect(await services.len('global-chat')).toBe(5)
    })

    test('leave', async () => {
        await services.leave('global-chat', 10013)
        expect(await services.len('global-chat')).toBe(5)
    })

    test('push message', async () => {
        await services.pushMessage('', 'chat', {}, 'global-chat2', {})
    })

    test('stop', async () => {
        await (services as any).stop()
    })
})
