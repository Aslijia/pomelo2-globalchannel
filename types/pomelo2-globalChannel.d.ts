

export interface GlobalChannelService {
    add(name: string, uid: string, sid: string, cb: Function);
    leave(name: string, uid: string, sid: string, cb: Function);
    getMembersBySid(name: string, sid: string, cb: Function);
    getMembersByChannelName(stype: string, name: string, cb: Function);
    pushMessage(serverType: string, route: string, msg: object, channelName: string, opts: object, cb: Function);
}