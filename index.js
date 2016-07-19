"use strict";
const restify = require('restify');
const Promise = require('bluebird');
const assert = require('assert-plus');

function ServiceNotFound(message) {
  this.message = message;
  this.name = "ServiceNotFound";
  Error.captureStackTrace(this, ServiceNotFound);
}
ServiceNotFound.prototype = Object.create(Error.prototype);
ServiceNotFound.prototype.constructor = ServiceNotFound;

const restifyClientOptions = [
	'accept',
	'connectTimeout',
	'requestTimeout',
	'dtrace',
	'gzip',
	'log',
	'retry',
	'signRequest',
	'userAgent',
	'version'
]

function parametersForService(service, opts) {
	return Promise.try(() => {
		let params = {
			service: service,
			passing: true
		}
		if (this.clientDefaults) {
			if (this.defaultTag) {
				params.tag = this.clientDefaults.tag;
			}
			if (this.defaultDc) {
				params.dc = this.clientDefaults.dc;
			}
		}
		if (opts) {
			if (opts.tag) {
				params.tag = opts.tag;
			}
			if (opts.dc) {
				params.dc = opts.dc;
			}
		}
		return params;
	})
}

function consulLookup(params) {
	return Promise.try(() => {
		assert.object(this.consul, 'consul');
		return this.consul.health.service(params)
	}).then((results) => {
		let endpoints = [];
		let endpointObj = {};
		results.forEach((result) => {
			let obj = {
				address: result.Service.Address,
				port: result.Service.Port,
				tags: result.Service.Tags,
				id: result.Service.ID,
				fullAddress: result.Service.Address + ':' + result.Service.Port,
			}
			if (endpointObj[obj.fullAddress] === undefined) {
				endpointObj[obj.fullAddress] = true;
				endpoints.push(obj);
			}
		});
		return endpoints;
	}).then((endpoints) => {
		if (endpoints.length === 0) {
			throw new ServiceNotFound(`Service ${params.service} has no endpoints`);
		} else {
			return endpoints;
		}
	})
}

function optionsForClient(endpoint, opts) {
	return Promise.try(() => {
		let resolvedOptions = {
			headers:{}
		};
		let urlObj = {
			scheme: 'http',
			path: ''
		}
		let optionSets = [];
		if (this.clientDefaults) {
			optionSets.push(this.clientDefaults);
		}
		if (opts) {
			optionSets.push(opts);
		}
		optionSets.forEach((set) => {
			Object.keys(set).forEach((setKey) => {
				if (setKey === 'scheme') {
					urlObj.scheme = set[setKey];
				} else if (setKey === 'path') {
					urlObj.path = set[setKey];
				} else if (setKey === 'headers') {
					resolvedOptions.headers = Object.assign(resolvedOptions.headers, set[setKey])
				} else if (restifyClientOptions.indexOf(setKey) !== -1) {
					resolvedOptions[setKey] = set[setKey];
				}
			});
		})
		let url = `${urlObj.scheme}://${endpoint.fullAddress}${urlObj.path}`;
		resolvedOptions['url'] = url;
		return resolvedOptions;
	})
}

function endpointsForService(service, opts) {
	return Promise.try(() => {
		return this.parametersForService(service, opts);
	}).then((params) => {
		return this.consulLookup(params);
	})
}

function singleEndpointForService(service, opts) {
	return this.endpointsForService(service, opts)
	.then((endpoints) => {
		if (endpoints.length === 1) {
			return endpoints[0];
		} else {
			return endpoints[Math.floor(Math.random()*endpoints.length)];
		}
	})
}

function clientForService(service, opts) {
	return Promise.try(() => {
		assert.string(service, 'service');
		return this.singleEndpointForService(service, opts);
	}).then((endpoint) => {
		return this.optionsForClient(endpoint, opts);
	}).then((options) => {
		let factoryMethod = restify.createJsonClient;
		if ((this.clientDefaults) && (this.clientDefaults.factory)) {
			factoryMethod = this.clientDefaults.factory;
		}
		if ((opts) && (opts.factory)) {
			factoryMethod = opts.factory;
		}
		if (this.promisify) {
			return Promise.promisifyAll(factoryMethod(options), {
				multiArgs: true
			});
		} else {
			return factoryMethod(options);
		}
	})
}

function clientForServices(services, opts) {
	return Promise.try(() => {
		assert.arrayOfString(services, 'services');
		let promises = services.map((serviceName) => {
			return this.clientForService(serviceName, opts);
		});
		return Promise.all(promises);
	})
}

exports.ServiceNotFoundError = ServiceNotFound;

exports.buildProvider = function(opts) {
	let obj = Object.assign({
		promisify: false,
		clientForServices: clientForServices,
		clientForService: clientForService,
		parametersForService: parametersForService,
		consulLookup: consulLookup,
		optionsForClient: optionsForClient,
		endpointsForService: endpointsForService,
		singleEndpointForService: singleEndpointForService
	}, opts);
	return obj;
}