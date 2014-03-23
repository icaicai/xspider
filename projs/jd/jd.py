
import PyV8



print '----------------------------------------------------------'
print get_var
print _download
print '----------------------------------------------------------'


class Global(PyV8.JSClass):
    @property
    def window(self):
        return self


    # print pc.product.tttttt


def domain_filter(value, *args):
    if '://item.jd.com' in value or '://list.jd.com' in value:
        return value

    return None

def urls_handler(rst, nxts, opts):
    urls = rst['urls']
    for url in urls:
        if '://item.jd.com' in url:
            nxts.append({'url': url, 'opts': {'rule': 'item'}})

        if '://list.jd.com' in url:
            nxts.append({'url': url, 'opts': {'rule': 'list'}})


def parse_item_info(nodes, name, values, item):
    # print '================================================================================'
    if len(nodes) < 1:
        return

    node = nodes[0]

    jscode = node.text_content()
    # print 'js content' ,jscode
    info = None
    obj = Global()
    with PyV8.JSContext(obj) as ctx:
        ctx.eval(jscode)
        info =  PyV8.convert(ctx.locals.pageConfig)

    prod = info['product']
    values.update(prod)
    # print values
    # print '================================================================================'


def save_product(result, opts):
    print '---------------------------------------------------------'
    print result
    print '---------------------------------------------------------'



'''
// 获得数字价格
var getPriceNum = function(skus, $wrap, perfix, callback) {
    skus = typeof skus === 'string' ? [skus]: skus;
    $wrap = $wrap || $('body');
    perfix = perfix || 'J-p-';
    $.ajax({
        url: 'http://p.3.cn/prices/mgets?skuIds=J_' + skus.join(',J_') + '&type=1',
        dataType: 'jsonp',
        success: function (r) {
            if (!r && !r.length) {
                return false;
            }
            for (var i = 0; i < r.length; i++) {
                var sku = r[i].id.replace('J_', '');
                var price = parseFloat(r[i].p, 10);

                if (price > 0) {
                    $wrap.find('.'+ perfix + sku).html('￥' + r[i].p + '');
                } else {
                    $wrap.find('.'+ perfix + sku).html('暂无报价');
                }

                if ( typeof callback === 'function' ) {
                    callback(sku, price, r);
                }
            }
        }
    });
};

'''    