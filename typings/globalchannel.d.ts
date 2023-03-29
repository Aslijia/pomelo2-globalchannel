declare module 'pomelo2' {
    interface Application {
        globalChannelService: globalChannelService
    }
}

declare interface globalChannelService {
    destroy(channel: string): Promise<void>

    len(channel: string): Promise<number>
    ismember(channel: string, uid: string | number): Promise<boolean>
    members(channel: string): Promise<(string | number)[]>
    members(channel: string, serverId: string): Promise<(string | number)[]>
    add(channel: string, uid: string | number, serverId: string): Promise<void>
    leave(channel: string, uid: string | number): Promise<void>
    pushMessage(serverType: string, route: string, msg: any, channel: string, opts: any): Promise<number | void>
}
