import React, { Component } from 'react';
import User from 'pages/Users/User';
// import 'App.css';
import Paper from '@material-ui/core/Paper';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableBody from '@material-ui/core/TableBody';
import TableRow from '@material-ui/core/TableRow';
import TableCell from '@material-ui/core/TableCell';
import CircularProgress from '@material-ui/core/CircularProgress';
import { withStyles } from '@material-ui/core/styles';
import UserAdd from 'pages/Users/UserAdd';

// const styles = theme => ({
//     root: {
//       width: "100%",
//       marginTop: theme.spacing(3),
//       overflowX: "auto"
//     },
//     table: {
//       minWidth: 1080
//     },
//     progress: {
//       margin: theme.spacing(2),
  
//     }
// });

export default class Users extends Component {
    constructor(props){
        super(props);
        this.state = { 
            userList : [],
            users: '',
            completed: 0
        };
        this.stateRefresh = this.stateRefresh.bind(this);
    } 

    stateRefresh() {
        this.setState({        
            users: '',      
            completed: 0        
        });      
        this.callApi()   
            .then(res => res.json())
            .then(data => this.setState({users:data.user_list}))        
            .catch(err => console.log(err));        
    }
        
    componentDidMount() {
        this.timer = setInterval(this.progress, 20);
        this.callApi()
          .then(res => this.setState({users: res}))
          .catch(err => console.log(err));
      }
    
      componentWillUnmount() {
        clearInterval(this.timer);
      }
    
    callApi = async () => {
    let api_url = "http://127.0.0.1:5000/users";
    let options = [];

    console.log("callapi")
    fetch(api_url)
        .then(res => res.json())
        .then(data => {
            console.log(data)
            this.setState({userList:data});
            this.setState({users:data.user_list});
        });
    // const body = await response.json();
    // return body;

    // const response = await fetch("http://127.0.0.1:5000/users");
    // const body = await response.json();
    // return body;
    }

    progress = () => {
        const { completed } = this.state;
        this.setState({ completed: completed >= 100 ? 0 : completed + 1 });
    };

    render() {
        const { classes } = this.props;
        const cellList = ["번호", "프로필 이미지", "이름", "생년월일", "성별", "직업", "설정"]

        return (
            <div>
                <UserAdd stateRefresh={this.stateRefresh} />
                <Paper >
                    <Table >
                    <TableHead>
                        <TableRow>
                            {cellList.map((c, index) => {return <TableCell key={index}>{c}</TableCell>})}
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {this.state.users ?
                            this.state.users.map((c, index) => {
                                let adrs = "https://placeimg.com/24/24/" + index
                                return <User stateRefresh={this.stateRefresh} key={index} id={c.id} image= {adrs} name={c.name} birthday={c.birthday} gender={c.gender} job={c.job} />
                            }) :
                            <TableRow>
                                <TableCell colSpan="6" align="center">
                                {/* <CircularProgress  variant="determinate" value={this.state.completed} /> */}
                                </TableCell>
                            </TableRow>
                        }
                    </TableBody>
                    </Table>
                </Paper>                
            </div>
        );
    }
}
