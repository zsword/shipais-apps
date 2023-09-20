import request from './apiclient'

export function sayHello() {
    return request({
        url: '/hello'
    })
}