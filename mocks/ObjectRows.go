package mocks

import "github.com/stretchr/testify/mock"

// ObjectRows is an autogenerated mock type for the ObjectRows type
type ObjectRows struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *ObjectRows) Close() {
	_m.Called()
}

// LastError provides a mock function with given fields:
func (_m *ObjectRows) LastError() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Next provides a mock function with given fields: _a0
func (_m *ObjectRows) Next(_a0 interface{}) (bool, error) {
	ret := _m.Called(_a0)

	var r0 bool
	if rf, ok := ret.Get(0).(func(interface{}) bool); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(interface{}) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}