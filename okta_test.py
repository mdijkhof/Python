
import requests

@f.retry(tries=3, delay=5, end_as_exception=True)
def sign_in(app: 'salesforce', s: requests.session = None) -> requests.session:

    if s is None:
        s = requests.session()

    try:
        # sign in to okta for session token
        h = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Origin': 'https://groupon.okta.com',
            'Referer': 'https://groupon.okta.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36',
        }

        s.headers.update(h)

        payload = {
            'username': 'mdijkhof@groupon.com',
            'password': 'seinfeld66P^',
            'options': {
                'multiOptionalFactorEnroll': 'false',
                'warnBeforePasswordExpired': 'false',
            }
        }

        r = s.post('https://groupon.okta.com/api/v1/authn', json=payload)
        f.timestamp('info', details=f'okta sign in status code: {r.status_code}]')
        session_token = r.json()['sessionToken']
    except Exception as e:
        f.timestamp('error', details=f'there was a problem obtaining session token from okta. message: {e}')
        raise

    try:
        # visit session redirect link for okta cookies
        session_cookie_redirect_url = f'https://groupon.okta.com/login/sessionCookieRedirect?checkAccountSetupComplete=true&token={session_token}&redirectUrl={APP_URL[app]}'
        r = s.get(session_cookie_redirect_url, allow_redirects=False)
        f.timestamp('info', details=f'session redirect link status code: {r.status_code}]')

        # use okta cookie to sign in to doorman app for cookies
        # this step is implemented in selenium webdriver to let Chrome handle all redirections
        driver = ch.chrome_driver(profile=f'{app}_profile', headless=True)
        driver.get('https://groupon.okta.com/')  # you need to be on the same domain as the cookie from requests session
        for cookie in s.cookies:
            cookie_dict = {'domain': cookie.domain, 'name': cookie.name, 'value': cookie.value, 'secure': cookie.secure}
            if cookie.expires:
                cookie_dict['expiry'] = cookie.expires
            if cookie.path_specified:
                cookie_dict['path'] = cookie.path

            driver.add_cookie(cookie_dict)

        # add doorman cookies to requests session and exit chrome
        driver.get(APP_URL[app])
        cookies = driver.get_cookies()
        for cookie in cookies:
            s.cookies.set(cookie['name'], cookie['value'])
    except Exception as e:
        f.timestamp('error', details=f'there was a problem signing in to okta with selenium webdriver. message: {e}')
        driver.close()
        raise

    driver.close()
    return s

sign_in('salesforce')