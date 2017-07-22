box.cfg{
    listen              = '~s',
    pid_file            = '~s',
    custom_proc_title   = 'taratest',
    work_dir = '~s'
}
fiber = require('fiber')
function testfunc(A, B, C) return A+B+C end
box.schema.func.create('testfunc', {if_not_exists = true})
function slowfunc(A, B, C) fiber.sleep(2) return (A+B+C) * 2 end
box.schema.func.create('slowfunc', {if_not_exists = true})


function get_s()
	return {'this is a long string, more then 31 byte'}
end
box.schema.func.create('get_s', {if_not_exists = true})

function get_small()
	return {'this is a short string'}
end
box.schema.func.create('get_small', {if_not_exists = true})


s = box.schema.space.create('testspace')
s:create_index('primary', {unique = true, parts = {1, 'NUM', 2, 'STR'}})
box.schema.user.create('manager', {if_not_exists = true, password = 'abcdef'})
box.schema.user.grant('manager', 'read,write,execute', 'universe')
