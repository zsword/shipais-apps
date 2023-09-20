import request from '../apiclient'

export function countByTimes() {
    return request({
        url: '/ais/countByTimes'
    })
}

export function cleanData(param) {
    return request({
        url: '/ais/cleanData',
        method: 'post',
        params: param
    })
}