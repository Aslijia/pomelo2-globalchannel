import IORedis from 'ioredis'
import { getLogger } from 'pomelo2-logger'

const logger = getLogger('globalchannel')

declare interface Application {
    set(k: string, v: any, bind: boolean): void
    getServersByType(sType: string): Array<{ id: string }>
    rpcInvoke(id: string, opts: any, cb?: Function): string[]
}

declare interface Options {
    url: string
    options?: {
        keyPrefix: string
        scaleReads?: string
    }
    cleanOnStartUp?: boolean
}

module.exports = function (app: Application, opts: Options) {
    const service = new ChannelService(app, opts)
    app.set('globalChannelService', service, true)
    return service
}

const DEFALT_PREFIX = '{POMELO-GLOBALCHANNEL}'

class ChannelManager {
    app: Application
    opts: Options
    redis: IORedis.Redis | undefined
    constructor(app: Application, opts: Options) {
        this.app = app
        this.opts = opts
        if (!this.opts.options) {
            this.opts.options = {
                keyPrefix: DEFALT_PREFIX
            }
        }
    }

    start(cb?: Function) {
        this.redis = new IORedis(this.opts.url, this.opts.options)

        this.redis.on('error', (err) => {
            logger.error('redis has error', { message: err.message })
        })

        this.redis.on('ready', () => {
            logger.trace('redis ready', { opts: this.opts })
            if (cb) cb()
        })
    }

    stop(force: boolean, cb?: Function) {
        if (!this.redis) {
            logger.error('clean', { force })
            return cb && typeof cb === 'function' && cb()
        }
        this.redis
            .quit()
            .then(() => {
                if (cb && typeof cb === 'function') cb()
            })
            .catch((err) => {
                if (cb && typeof cb === 'function') cb(err)
            })
    }

    async cleanup() {
        if (!this.redis) {
            logger.error('clean', {})
            return
        }
        let cleaned: string[] = []
        let cursor = '0'
        const keyPrefix = this.opts.options ? this.opts.options.keyPrefix : ''
        do {
            const batch = await this.redis.scan(cursor, 'MATCH', `${keyPrefix}*`, 'COUNT', 100)
            const elements = batch[1]
            if (elements && elements.length) {
                const keys = elements.filter((k) => k && k.replace(keyPrefix, ''))
                if (keys && keys.length) {
                    this.redis.del(...keys)
                    cleaned = cleaned.concat(keys)
                }
            }
            cursor = batch[0]
        } while (cursor != '0')
        logger.warn('global channel was clean', { keys: cleaned })
    }

    async destroy(channel: string, cb?: Function) {
        if (!this.redis) {
            logger.error('destroyChannel', {})
            return cb && typeof cb === 'function' && cb()
        }

        const members = await this.members(channel)
        if (members) {
            members.forEach((uid) => this.leave(channel, uid))
        }
    }

    async len(channel: string) {
        if (!this.redis) {
            logger.error('reids service invalid', { channel })
            return
        }
        return await this.redis.hlen(`c#${channel}`)
    }

    async add(name: string, uid: number | string, sid: string) {
        if (!this.redis) {
            logger.error('reids service invalid', { name, uid })
            return
        }
        const o = await this.redis.hget(`u#${uid}`, name)
        if (o) await this.redis.srem(`s#${name}:${sid}`, uid)

        await this.redis.hset(`u#${uid}`, name, sid)
        await this.redis.sadd(`s#${name}:${sid}`, uid.toString())
        await this.redis.hset(`c#${name}`, uid.toString(), sid)
        console.log('add one:', name, uid, sid)
    }

    async leave(channel: string, uid: number | string, sid?: string | null) {
        if (!this.redis) {
            return
        }
        sid = sid || (await this.redis.hget(`u#${uid}`, channel))
        this.redis.hdel(`u#${uid}`, channel)
        this.redis.hdel(`c#${channel}`, uid as string)
        if (sid) this.redis.srem(`s#${channel}:${sid}`, uid)
    }

    async members(channel: string, serverId?: string) {
        if (!this.redis) {
            return
        }
        if (serverId) {
            return await this.redis.smembers(`s#${channel}:${serverId}`)
        }
        return (await this.redis.hkeys(`c#${channel}`)) || []
    }

    async ismember(channel: string, uid: number | string) {
        if (!this.redis) {
            return
        }
        return await this.redis.hexists(`c#${channel}`, uid as string)
    }

    async channels(uid: number | string) {
        logger.trace('getChannelsByUid', { uid })
        if (!this.redis) {
            return []
        }
        return await this.redis.hkeys(`u#${uid}`)
    }
}

enum STATE {
    ST_INITED = 0,
    ST_STARTED = 1,
    ST_CLOSED = 2
}

/**
 * Global channel service.
 * GlobalChannelService is created by globalChannel component which is a default
 * component of pomelo enabled by `app.set('globalChannelConfig', {...})`
 * and global channel service would be accessed by
 * `app.get('globalChannelService')`.
 *
 * @class
 * @constructor
 */
class ChannelService {
    app: Application
    manager: ChannelManager
    state: STATE
    opts: any
    name: string = '__globalChannel__'
    constructor(app: Application, opts: any) {
        this.app = app
        this.opts = opts

        if (opts.manager) {
            this.manager = opts.manager
        } else {
            this.manager = new ChannelManager(app, opts)
        }
        this.state = STATE.ST_INITED
        logger.debug('create ChannelService', {})
    }

    start(cb?: Function) {
        this.manager.start(() => {
            this.state = STATE.ST_STARTED
            if (this.opts.cleanOnStartUp) {
                this.manager.cleanup().then(() => {
                    cb && typeof cb === 'function' && cb()
                })
            } else {
                cb && typeof cb === 'function' && cb()
            }
        })
    }

    stop(force: boolean, cb?: Function) {
        this.state = STATE.ST_CLOSED
        this.manager.stop(force, cb)
    }

    async destroy(name: string) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('destroy channel failed', { name, state: this.state })
            return
        }
        await this.manager.destroy(name)
    }

    async len(channel: string) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('get channel len failed', { channel, state: this.state })
            return
        }
        return await this.manager.len(channel)
    }

    async add(channel: string, uid: number, serverId: string) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('add member failed', { channel, uid, serverId, state: this.state })
            return
        }
        await this.manager.add(channel, uid, serverId)
    }

    async leave(channel: string, uid: number, sid?: string) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('leave member failed', { channel, uid, sid, state: this.state })
            return
        }
        await this.manager.leave(channel, uid, sid)
    }

    async members(channel: string, serverId?: string) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('get members failed', { channel, state: this.state })
            return
        }
        return await this.manager.members(channel, serverId)
    }

    async ismember(channel: string, uid: string | number) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('get ismember failed', { channel, state: this.state })
            return
        }
        return await this.manager.ismember(channel, uid)
    }

    async channels(uid: string | number) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('get channels failed', { uid, state: this.state })
            return
        }
        return await this.manager.channels(uid)
    }

    async pushMessage(serverType: string, route: string, msg: any, channelName: string, opts: any) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('pushMessage failed', { serverType, route, channelName, opts, state: this.state })
            return
        }

        const namespace = 'sys'
        const service = 'channelRemote'
        const method = 'pushMessage'
        const servers = this.app.getServersByType(serverType)
        if (!servers || servers.length === 0) {
            logger.warn('no frontend server infos', { serverType, servers })
            return
        }
        let users: string[] = []
        for (let i in servers) {
            const uids = await this.members(channelName, servers[i].id)
            if (uids && uids.length) {
                users = users.concat(uids)
                const params = {
                    namespace: namespace,
                    service: service,
                    method: method,
                    args: [route, msg, uids, { isPush: true }]
                }
                this.app.rpcInvoke(servers[i].id, params, () => {})
            }
        }
        logger.debug('global channel pushmessage', { serverType, route, msg, channelName, users })
        return users
    }
}
