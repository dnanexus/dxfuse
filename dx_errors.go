package dxfuse

import (
	"strings"
)

// Errors, based on https://github.com/dnanexus/nucleus/blob/master/commons/api_error/api_error.js
//
type DxError struct {
	name string
	code int
	defaultMessage string
	httpCode int
}

// Figure out which error we got
func DxErrorParse(err error) int {
	msg := err.Error()

	// This check should be done on the returned http body.
	for _, dxe := range(apiErrors) {
		if strings.Contains(msg, dxe.defaultMessage) {
			return dxe.code
		}
	}
	if strings.Contains(msg, "Unauthorized") {
		return Unauthorized
	}

	return OtherError
}

const (
	MovedPermanently int = iota + 1
	MovedTemporarily
	BadRequest
	MalformedJSON
	BadDigest
	InvalidAuthentication
	PermissionDenied
	Unauthorized
	ForbiddenClient
	SpendingLimitExceeded
	ClientTooOld
	ResourceNotFound
	MethodNotAllowed
	RequestTimeout
	Conflict
	LengthRequired
	PreconditionFailed
	RequestedRangeNotSatisfiable
	InvalidInput
	InvalidState
	InvalidType
	RateLimitConditional
	InternalServerError
	ServiceUnavailable
	InternalError

	// Unrecognized error code, something else
	OtherError
)

// an array of all possible dnanexus errors
var apiErrors []DxError = []DxError{
	DxError{
		name : "MovedPermanently",
		code : MovedPermanently,
		defaultMessage: "The requested resource has been moved permanently",
		httpCode: 301,
	},
	DxError{
		name : "MovedTemporarily",
		code : MovedTemporarily,
		defaultMessage: "The requested resource has been moved temporarily",
		httpCode: 307,
	},
	DxError{
		name : "BadRequest",
		code : BadRequest,
		defaultMessage: "The request is invalid due to bad syntax",
		httpCode: 400,
	},
	DxError{
		name : "MalformedJSON",
		code : MalformedJSON,
		defaultMessage: "The input could not be parsed as JSON",
		httpCode: 400,
	},
	DxError{
		name : "BadDigest",
		code : BadDigest,
		defaultMessage: "The Content-MD5 you specified did not match what we received",
		httpCode: 400,
	},
	DxError{
		name : "InvalidAuthentication",
		code : InvalidAuthentication,
		defaultMessage: "The provided access token is invalid",
		httpCode: 401,
	},
	DxError{
		name : "PermissionDenied",
		code : PermissionDenied,
		defaultMessage: "Insufficient permissions to perform this action",
		httpCode: 401,
	},
	DxError{
		name : "Unauthorized",
		code : Unauthorized,
		defaultMessage: "Invalid authentication used",
		httpCode: 401,
	},
	DxError{
		name : "ForbiddenClient",
		code : ForbiddenClient,
		defaultMessage: "The client you are using is not authorized to perform this action",
		httpCode: 403,
	},
	DxError{
		name : "SpendingLimitExceeded",
		code : SpendingLimitExceeded,
		defaultMessage: "The spending limit has been reached for the account that would be billed for this action",
		httpCode: 403,
	},
	DxError{
		name : "ClientTooOld",
		code : ClientTooOld,
		defaultMessage: "The 'User-Agent' header indicates that your client/SDK version is too old. Unable to continue with the request",
		httpCode: 403,
	},
	DxError{
		name : "ResourceNotFound",
		code : ResourceNotFound,
		defaultMessage: "A specified entity or resource could not be found",
		httpCode: 404,
	},
	DxError{
		name : "MethodNotAllowed",
		code : MethodNotAllowed,
		defaultMessage: "Method not allowed",
		httpCode: 405,
	},
	DxError{
		name : "RequestTimeout",
		code : RequestTimeout,
		defaultMessage: "Unable to complete the request within the expected time.",
		httpCode: 408,
	},
	DxError{
		name : "Conflict",
		code : Conflict,
		defaultMessage: "Unable to satisfy the request on the resource due to a conflict",
		httpCode: 409,
	},
	DxError{
		name : "LengthRequired",
		code : LengthRequired,
		defaultMessage: "Content length not specified",
		httpCode: 411,
	},
	DxError{
		name : "PreconditionFailed",
		code : PreconditionFailed,
		defaultMessage: "One of the conditions the request was made under has failed",
		httpCode: 412,
	},
	DxError{
		name : "RequestedRangeNotSatisfiable",
		code : RequestedRangeNotSatisfiable,
		defaultMessage: "The requested range did not overlap the extent of the resource",
		httpCode: 416,
	},
	DxError{
		name : "InvalidInput",
		code : InvalidInput,
		defaultMessage: "The input is semantically incorrect",
		httpCode: 422,
	},
	DxError{
		name : "InvalidState",
		code : InvalidState,
		defaultMessage: "The operation is not allowed at this object state",
		httpCode: 422,
	},
	DxError{
		name : "InvalidType",
		code : InvalidType,
		defaultMessage: "An object specified in the request is of invalid type",
		httpCode: 422,
	},
	DxError{
		name : "RateLimitConditional",
		code : RateLimitConditional,
		defaultMessage: "Too many invalid requests",
		httpCode: 429,
	},
	DxError{
		name : "InternalServerError",
		code : InternalServerError,
		defaultMessage: "An unexpected error occurred while processing the request; please try again later",
		httpCode: 500,
	},
	DxError{
		name : "ServiceUnavailable",
		code : ServiceUnavailable,
		defaultMessage: "Some resource was temporarily unavailable; please try again later",
		httpCode: 503,
	},
	DxError{
		name : "InternalError",
		code : InternalError,
		defaultMessage: "The server encountered an internal error",
		httpCode: 500,
	},
}
