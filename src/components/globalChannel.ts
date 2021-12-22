import IORedis from 'ioredis'
import { getLogger } from 'pomelo2-logger'
import { promisify } from 'util'

const logger = getLogger('globalchannel')

declare interface Application {
    set(k: string, v: any, bind: boolean): void
    getServers(): { [id: string]: object }
    isFrontend(obj: object): boolean
    getServersByType(sType: string): Array<{ id: string }>
    rpcInvoke(id: string, opts: any, cb?: Function): string[]
}

module.exports = function (app: Application, opts: any) {
    const service = new ChannelService(app, opts)
    app.set('globalChannelService', service, true)
    return service
}

const DEFALT_PREFIX = '{POMELO-GLOBALCHANNEL}'

class ChannelManager {
    app: Application
    opts: any
    redis: IORedis.Redis | undefined
    constructor(app: Application, opts: any) {
        this.app = app
        this.opts = opts
        if (!this.opts.options) {
            this.opts.options = {
                keyPrefix: DEFALT_PREFIX
            }
        }
        logger.trace('constructor', { opts })
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
            return cb && cb()
        }
        this.redis.quit()
        if (cb) cb()
    }

    async clean() {
        if (!this.redis) {
            logger.error('clean', {})
            return
        }

        const keys = await this.redis.keys(`${this.opts.options.keyPrefix}*`)
        if (!keys) return

        const cmds: string[] = []
        keys.forEach((k: string) => {
            cmds.push(this.opts.options.keyPrefix ? k.replace(this.opts.options.keyPrefix, '') : k)
        })
        if (cmds.length) await this.redis.del(...cmds)

        logger.warn('global channel was clean', { keys })
    }

    async destroyChannel(name: string, cb?: Function) {
        if (!this.redis) {
            logger.error('destroyChannel', {})
            return cb && cb()
        }

        const servers = this.app.getServers()
        const cmds: Array<string[]> = []
        for (var sid in servers) {
            const server = servers[sid]
            if (this.app.isFrontend(server)) {
                cmds.push(['del', `${name}${sid}`])
            }
        }
        if (cmds.length) await this.redis.multi(cmds).exec()
        logger.debug('the channel destoryed', { name })
    }

    async add(name: string, uid: number, sid: string) {
        if (!this.redis) {
            return
        }
        logger.trace('add', { name, uid, sid })
        await this.redis.sadd(`user.${uid}.sids`, JSON.stringify({ name, sid }))
        await this.redis.sadd(`${name}:${sid}`, uid.toString())
        await this.redis.hset(name, uid.toString(), sid)
    }

    async leave(name: string, uid: number, sid?: string) {
        if (!this.redis) {
            return
        }
        if (!sid) {
            const members = await this.redis.smembers(`user.${uid}.sids`)
            for (let i in members) {
                try {
                    const member = typeof members[i] === 'string' ? JSON.parse(members[i]) : members[i]
                    if (member && member.name === name) {
                        logger.trace('leave', { name, uid, member })
                        await this.redis.srem(`user.${uid}.sids`, JSON.stringify(member))
                        await this.redis.srem(`${name}:${member.sid}`, uid.toString())
                        await this.redis.hdel(name, uid.toString())
                    }
                } catch (err: any) {
                    logger.error('parse channel failed', {})
                }
            }
        } else {
            logger.trace('leave', { name, uid, sid })
            await this.redis.srem(`user.${uid}.sids`, JSON.stringify({ name, sid }))
            await this.redis.srem(`${name}:${sid}`, uid.toString())
            await this.redis.hdel(name, uid.toString())
        }
    }

    async members(name: string) {
        if (!this.redis) {
            return
        }
        return (await this.redis.hkeys(name)) || []
    }

    async ismember(name: string, uid: number) {
        if (!this.redis) {
            return
        }
        return await this.redis.hexists(name, uid.toString())
    }

    async getMembersBySid(name: string, sid: string) {
        if (!this.redis) {
            return
        }
        logger.trace('getMembersBySid', { name, sid })
        return await this.redis.smembers(`${name}:${sid}`)
    }

    async getChannelsByUid(uid: number) {
        logger.trace('getChannelsByUid', { uid })
        if (!this.redis) {
            return []
        }

        const channels = await this.redis.smembers(`user.${uid}.sids`)
        if (!channels) {
            return []
        }

        for (let i in channels) {
            try {
                channels[i] = JSON.parse(channels[i])
            } catch (_) {}
        }
        logger.debug('getChannelsByUid', { uid, channels })
        return channels
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
                this.manager.clean().then(() => {
                    cb && cb()
                })
            } else {
                cb && cb()
            }
        })
    }

    stop(force: boolean, cb?: Function) {
        this.state = STATE.ST_CLOSED
        this.manager.stop(force, cb)
    }

    /**
     * Destroy a global channel.
     *
     * @param  {String}   name global channel name
     * @param  {Function} cb callback function
     *
     * @memberOf GlobalChannelService
     */
    async destroyChannel(name: string) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('destroy channel failed', { name, state: this.state })
            return
        }
        await this.manager.destroyChannel(name)
    }

    /**
     * Add a member into channel.
     *
     * @param  {String}   name channel name
     * @param  {String}   uid  user id
     * @param  {String}   sid  frontend server id
     * @param  {Function} cb   callback function
     *
     * @memberOf GlobalChannelService
     */
    async add(name: string, uid: number, sid: string) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('add member failed', {
                name,
                uid,
                sid,
                state: this.state
            })
            return
        }
        await this.manager.add(name, uid, sid)
    }

    /**
     * Remove user from channel.
     *
     * @param  {String}   name channel name
     * @param  {String}   uid  user id
     * @param  {String}   sid  frontend server id
     * @param  {Function} cb   callback function
     *
     * @memberOf GlobalChannelService
     */
    async leave(name: string, uid: number, sid?: string) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('leave member failed', {
                name,
                uid,
                sid,
                state: this.state
            })
            return
        }
        await this.manager.leave(name, uid, sid)
    }

    /**
     * Get members by frontend server id.
     *
     * @param  {String}   name channel name
     * @param  {String}   sid  frontend server id
     * @param  {Function} cb   callback function
     *
     * @memberOf GlobalChannelService
     */
    async getMembersBySid(name: string, sid: string) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('getMembersBySid failed', {
                name,
                sid,
                state: this.state
            })
            return []
        }
        return await this.manager.getMembersBySid(name, sid)
    }

    async getMembersByChannel(name: string) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('leave member failed', {
                name,
                state: this.state
            })
            return []
        }
        return (await this.manager.members(name)) || []
    }

    async isMemberInChannel(name: string, uid: number) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('leave member failed', {
                name,
                uid,
                state: this.state
            })
            return false
        }
        return (await this.manager.ismember(name, uid)) || false
    }

    /**
     * Get members by channel name.
     *
     * @param  {String}   stype frontend server type string
     * @param  {String}   name channel name
     * @param  {Function} cb   callback function
     *
     * @memberOf GlobalChannelService
     */
    async getMembersByChannelName(stype: string, name: string) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('getMembersByChannelName failed', {
                name,
                stype,
                state: this.state
            })
            return
        }

        const servers = this.app.getServersByType(stype)
        if (!servers || servers.length === 0) {
            return []
        }

        let members: string[] = []
        for (let i = 0; i < servers.length; i++) {
            const server = servers[i]
            const m = await this.getMembersBySid(name, server.id)
            if (m && m.length) {
                members = members.concat(m)
            }
        }
        return members
    }

    /**
     * get joined channel list
     */
    async getChannelsByMember(uid: number) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('getMembersByChannelName failed', {
                uid,
                state: this.state
            })
            return []
        }
        return await this.manager.getChannelsByUid(uid)
    }

    /**
     * Send message by global channel.
     *
     * @param  {String}   serverType  frontend server type
     * @param  {String}   route       route string
     * @param  {Object}   msg         message would be sent to clients
     * @param  {String}   channelName channel name
     * @param  {Object}   opts        reserved
     * @param  {Function} cb          callback function
     *
     * @memberOf GlobalChannelService
     */
    async pushMessage(serverType: string, route: string, msg: any, channelName: string, opts: any) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('pushMessage failed', {
                name,
                serverType,
                route,
                channelName,
                opts,
                state: this.state
            })
            return
        }

        const namespace = 'sys'
        const service = 'channelRemote'
        const method = 'pushMessage'
        let failIds: string[] = []

        const servers = this.app.getServersByType(serverType)
        if (!servers || servers.length === 0) {
            logger.warn('no frontend server infos', { serverType, servers })
            return
        }

        let users: string[] = []
        for (let i in servers) {
            const uids = await this.getMembersBySid(channelName, servers[i].id)
            if (uids && uids.length) {
                users = users.concat(uids)

                const params = {
                    namespace: namespace,
                    service: service,
                    method: method,
                    args: [route, msg, uids, { isPush: true }]
                }

                const fails = <string[]>await promisify(this.app.rpcInvoke.bind(this.app))(
                    servers[i].id,
                    //@ts-ignore
                    params
                )
                if (fails) failIds = failIds.concat(fails)
            }
        }
        logger.debug('global channel pushmessage failIds', {
            serverType,
            route,
            msg,
            channelName,
            failIds,
            users
        })
        return failIds
    }
}
