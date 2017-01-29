import React from 'react';
import AppBar from 'material-ui/AppBar';
import FullWidthSection from './FullWidthSection';
import Drawer from 'material-ui/Drawer';
import { Link } from 'react-router';
import {spacing, typography, zIndex} from 'material-ui/styles';
import {cyan500} from 'material-ui/styles/colors';

const styles = {
    logo: {
        cursor: 'pointer',
        fontSize: 24,
        color: typography.textFullWhite,
        lineHeight: `${spacing.desktopKeylineIncrement}px`,
        fontWeight: typography.fontWeightLight,
        backgroundColor: cyan500,
        paddingLeft: spacing.desktopGutter,
        marginBottom: spacing.desktopKeylineIncrement,
    },
    navItem: {
        fontSize: 18,
        color: typography.textLightBlack,
        lineHeight: `${spacing.desktopDrawerMenuItemHeight}px`,
        fontWeight: typography.fontWeightLight,
        paddingLeft: spacing.desktopGutter,
        textDecoration: 'none'
    }
};

class Container extends React.Component {
    state = {
        navDrawerOpen: false,
    };

    handleTouchTapLeftIconButton = () => {
        this.setState({
            navDrawerOpen: !this.state.navDrawerOpen,
        });
    };

    render() {
        let {
            navDrawerOpen,
        } = this.state;

        return (
            <div>
                <AppBar title="Eventor" onLeftIconButtonTouchTap={this.handleTouchTapLeftIconButton} showMenuIconButton={true}/>
                <Drawer docked={false} onRequestChange={(open) => this.handleTouchTapLeftIconButton()} open={navDrawerOpen}>
                    <div style={styles.logo}>
                        Eventor
                    </div>
                    <div><Link to="/streams" style={styles.navItem}>Streams</Link></div>
                    <div><Link to="/streams" style={styles.navItem}>Subscriptions</Link></div>
                </Drawer>
                <FullWidthSection useContent={true}>
                    {this.props.children}
                </FullWidthSection>
            </div>
        );
    }
}

export default Container;
