import React from 'react';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogTitle from '@material-ui/core/DialogTitle';
import DialogContent from '@material-ui/core/DialogContent';
import DialogActions from '@material-ui/core/DialogActions';
import Typography from '@material-ui/core/Typography';

class UserDelete extends React.Component {

    constructor(props) {
        super(props);
            this.state = {
            open: false
        }
        this.handleClickOpen = this.handleClickOpen.bind(this)
        this.handleClose = this.handleClose.bind(this);
    }
        
    handleClickOpen() {
        this.setState({
            open: true
        });
    }

    handleClose() {
        this.setState({
            open: false
        })
    }

    deleteUser(id){
        const url = '/user/' + id;
        fetch(url, {
            method: 'DELETE'
        })
        .then((response) => {
            this.props.stateRefresh();
        });
        
    }

    render() {
        return (
            <div>   
                <Button variant="contained" color="default" onClick={this.handleClickOpen}>삭제</Button>
                <Dialog onClose={this.handleClose} open={this.state.open}>
                    <DialogTitle onClose={this.handleClose}>삭제확인</DialogTitle>
                    <DialogContent>
                        <Typography gutterBottom>선택한 고객 정보가 삭제됩니다.</Typography>
                    </DialogContent>
                    <DialogActions>
                        <Button variant="contained" color="primary" onClick={(e) => {this.deleteUser(this.props.id)}}>삭제</Button>
                        <Button variant="outlined" color="primary" onClick={this.handleClose}>닫기</Button>
                    </DialogActions>
                 </Dialog>
            </div>
        )
    }
}

export default UserDelete;
