import { RedisClient } from '@aslijia/redis';
import { getLogger } from 'pomelo2-logger';
import { promisify } from 'util';

const logger = getLogger('globalchannel');
declare interface Application {
    set(k: string, v: any, bind: boolean): void;
    getServers(): { [id: string]: object }
    isFrontend(obj: object): boolean;
    getServersByType(sType: string): Array<{ id: string }>;
    rpcInvoke(id: string, opts: any, cb?: Function): string[];
}

module.exports = function (app: Application, opts: any) {
    const service = new ChannelService(app, opts);
    app.set('globalChannelService', service, true);
    return service;
}

class ChannelManager {
    app: Application;
    opts: any;
    redis: RedisClient | undefined;
    constructor(app: Application, opts: any) {
        this.app = app;
        this.opts = opts;
        logger.debug('init ChannelManager', { opts });
    }

    start(cb?: Function) {
        this.redis = new RedisClient(this.opts);
        this.redis.on('error', (err) => {
            logger.error('redis has error', { message: err.message });
        });
        this.redis.on('ready', () => {
            logger.info('globalchannel init', { opts: this.opts });
            if (cb) cb();
        });
    }

    stop(force: boolean, cb?: Function) {
        this.redis?.quit();
        if (cb) cb();
    }

    async clean() {
        const keys = await this.redis?.keys(`${this.opts.prifix || ''}*`);
        if (!keys) return;

        const cmds: string[] = [];
        keys.forEach((k: string) => {
            cmds.push(this.opts.prifix ? k.replace(this.opts.prifix, '') : k);
        });
        if (cmds.length)
            await this.redis?.del(...cmds);
        logger.warn('global channel was clean', { keys });
    }

    async destroyChannel(name: string, cb?: Function) {
        const servers = this.app.getServers();
        const cmds: Array<string[]> = [];
        for (var sid in servers) {
            const server = servers[sid];
            if (this.app.isFrontend(server)) {
                cmds.push(['del', `${name}${sid}`]);
            }
        }
        if (cmds.length)
            await this.redis?.multi(cmds).exec();
        logger.debug('the channel destoryed', { name });
    }

    async add(name: string, uid: number, sid: string) {
        return await this.redis?.sadd(`${name}:${sid}`, uid.toString());
    }

    async leave(name: string, uid: number, sid: string) {
        return await this.redis?.srem(`${name}:${sid}`, uid.toString());
    }

    async getMembersBySid(name: string, sid: string) {
        return await this.redis?.smembers(`${name}:${sid}`);
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
    app: Application;
    manager: ChannelManager;
    state: STATE;
    opts: any;
    name: string = '__globalChannel__';
    constructor(app: Application, opts: any) {
        this.app = app;
        this.opts = opts;

        if (opts.manager) {
            this.manager = opts.manager;
        } else {
            this.manager = new ChannelManager(app, opts);
        }
        this.state = STATE.ST_INITED;
        logger.debug('create ChannelService', {});
    }

    start(cb?: Function) {
        this.manager.start(() => {
            this.state = STATE.ST_STARTED;
            if (this.opts.cleanOnStartUp) {
                this.manager.clean().then(() => {
                    cb && cb();
                });
            } else {
                cb && cb();
            }
        });
    }

    stop(force: boolean, cb?: Function) {
        this.state = STATE.ST_CLOSED;
        this.manager.stop(force, cb);
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
            logger.error('destroy channel failed', { name, state: this.state });
            return;
        }
        await this.manager.destroyChannel(name);
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
            logger.error('add member failed', { name, uid, sid, state: this.state });
            return;
        }
        await this.manager.add(name, uid, sid);
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
    async leave(name: string, uid: number, sid: string) {
        if (this.state !== STATE.ST_STARTED) {
            logger.error('leave member failed', { name, uid, sid, state: this.state });
            return;
        }
        await this.manager.leave(name, uid, sid);
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
            logger.error('getMembersBySid failed', { name, sid, state: this.state });
            return [];
        }
        await this.manager.getMembersBySid(name, sid);
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
            logger.error('getMembersByChannelName failed', { name, stype, state: this.state });
            return;
        }

        const servers = this.app.getServersByType(stype);
        if (!servers || servers.length === 0) {
            return [];
        }

        let members: string[] = [];
        for (let i = 0; i < servers.length; i++) {
            const server = servers[i];
            const m = await this.getMembersBySid(name, server.id);
            if (m && m.length) {
                members = members.concat(m);
            }
        }
        return members;
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
            logger.error('pushMessage failed', { name, serverType, route, channelName, opts, state: this.state });
            return;
        }

        const namespace = 'sys';
        const service = 'channelRemote';
        const method = 'pushMessage';
        let failIds: string[] = [];

        const servers = this.app.getServersByType(serverType);
        if (!servers || servers.length === 0) {
            logger.warn('no frontend server infos', { serverType, servers });
            return;
        }

        for (let i in servers) {
            const uids = await this.getMembersBySid(channelName, servers[i].id);
            const fails = <string[]>await promisify(this.app.rpcInvoke.bind(this.app))(servers[i].id, {
                namespace: namespace, service: service, method: method, args: [route, msg, uids, { isPush: true }]
            });
            if (fails)
                failIds = failIds.concat(fails);
        }
        logger.debug('global channel pushmessage failIds', { serverType, route, msg, channelName, failIds });
        return failIds;
    }
}