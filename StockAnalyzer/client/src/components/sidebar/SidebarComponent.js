import React from 'react';
import { createUseStyles, useTheme } from 'react-jss';
import { useHistory } from 'react-router-dom';
import SLUGS from 'resources/slugs';
import {
    IconAgents,
    IconArticles,
    IconContacts,
    IconIdeas,
    IconLogout,
    IconOverview,
    IconSettings,
    IconSubscription,
    IconTickets,
    IconBriefcase
} from 'assets/icons';
import { convertSlugToUrl } from 'resources/utilities';
import LogoComponent from './LogoComponent';
import Menu from './MenuComponent';
import MenuItem from './MenuItemComponent';

const useStyles = createUseStyles({
    separator: {
        borderTop: ({ theme }) => `1px solid ${theme.color.lightGrayishBlue}`,
        marginTop: 12,
        marginBottom: 12,
        opacity: 0.06
    }
});

function SidebarComponent() {
    const { push } = useHistory();
    const theme = useTheme();
    const classes = useStyles({ theme });
    // const isMobile = window.innerWidth <= 1080;
    const isMobile = window.innerWidth <= 2080;

    async function logout() {
        push(SLUGS.login);
    }

    function onClick(slug, parameters = {}) {
        push(convertSlugToUrl(slug, parameters));
    }

    return (
        <Menu isMobile={isMobile}>
            <div style={{ paddingTop: 30, paddingBottom: 10 }}>
                <LogoComponent />
            </div>
            <MenuItem id={SLUGS.dashboard}  title='Dashboard' icon={IconSubscription} onClick={() => onClick(SLUGS.dashboard)} />
            <MenuItem id={SLUGS.overview} items={[SLUGS.overviewTwo, SLUGS.overviewThree]} title='AI Predict' icon={IconOverview} >
                <MenuItem id={SLUGS.overview} title='예측작업실행' level={2} icon={IconAgents} onClick={() => onClick(SLUGS.overview)} />
                <MenuItem id={SLUGS.overviewTwo} title='Sub Item 2' level={2} icon={IconContacts} onClick={() => onClick(SLUGS.overviewTwo)}/>
                <MenuItem id={SLUGS.overviewThree} title='Sub Item 3' level={2} icon={IconArticles} onClick={() => onClick(SLUGS.overviewThree)} />
                <MenuItem id={SLUGS.overviewFour} title='Sub Item 4' level={2} icon={IconBriefcase} onClick={() => onClick(SLUGS.overviewFour)} />
            </MenuItem>
            <MenuItem id={SLUGS.reports} items={[SLUGS.reportsTwo, SLUGS.reportsThree]} title='AI Reports' icon={IconTickets}>
                <MenuItem id={SLUGS.reports} title='예측수익률 TOP10' level={2} icon={IconAgents} onClick={() => onClick(SLUGS.reports)} />
                <MenuItem id={SLUGS.reportsTwo} title='예상과 실적 비교' level={2} icon={IconContacts} onClick={() => onClick(SLUGS.reportsTwo)}/>
                <MenuItem id={SLUGS.reportsThree} title='Sub Item 3' level={2} icon={IconArticles} onClick={() => onClick(SLUGS.reportsThree)} />
            </MenuItem>
            <MenuItem id={SLUGS.ideas}  items={[SLUGS.ideasTwo, SLUGS.ideasThree]}  title='주식거래'  icon={IconIdeas} >
                <MenuItem id={SLUGS.ideas}      title='주문(예약주문)' level={2} icon={IconAgents} onClick={() => onClick(SLUGS.ideas)} />
                <MenuItem id={SLUGS.ideasTwo}   title='AI거래조건설정' level={2} icon={IconContacts} onClick={() => onClick(SLUGS.ideasTwo)} />
                <MenuItem id={SLUGS.ideasThree} title='AI 거래'       level={2} icon={IconArticles} onClick={() => onClick(SLUGS.ideasThree)} />
            </MenuItem>
            <MenuItem id={SLUGS.contacts} title='계좌정보' icon={IconContacts} onClick={() => onClick(SLUGS.contacts)} />
            <MenuItem id={SLUGS.agents} title='Agents' icon={IconAgents} onClick={() => onClick(SLUGS.agents)} />
            <MenuItem id={SLUGS.articles} title='Articles' icon={IconArticles} onClick={() => onClick(SLUGS.articles)} />
            <MenuItem id={SLUGS.subscription} title='Schedule' icon={IconSubscription} onClick={() => onClick(SLUGS.subscription)} />
            <div className={classes.separator}></div>
            <MenuItem id={SLUGS.settings} title='Settings' icon={IconSettings} onClick={() => onClick(SLUGS.settings)} />
            <MenuItem id='logout' title='Logout' icon={IconLogout} onClick={logout} />
        </Menu>
    );
}

export default SidebarComponent;
